#define DUCKDB_EXTENSION_MAIN

#include "radix_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/vector.hpp"

#include "builder.h"
#include "radix_spline.h"
#include "serializer.h"
#include "common.h"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

const size_t kNumRadixBits = 18;
const size_t kMaxError = 32;

// Separate maps for different key types, for each table
std::map<std::string, rs::RadixSpline<uint32_t>> radix_spline_map_int32;
std::map<std::string, rs::RadixSpline<uint64_t>> radix_spline_map_int64;

inline void RadixScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Radix "+name.GetString()+" 🐥");;
        });
}

inline void RadixOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Radix " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

/*
Bulk Load into Index functions
*/
template <typename T>
void BulkLoadRadixSpline(duckdb::Connection &con, const std::string &table_name, int column_index, const std::string &map_key) {
    // Ensure T is one of the allowed types
    static_assert(std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
                  "BulkLoadRadixSpline only supports uint32_t and uint64_t.");

    // Query the table
    std::string query = "SELECT * FROM " + table_name + ";";
    unique_ptr<MaterializedQueryResult> res = con.Query(query);

    if (!res || res->HasError()) {
        throw std::runtime_error("Failed to fetch data from table: " + table_name);
    }

    // Fetch results
    auto result = res->getContents();

    // Pre-allocate keys
    std::vector<T> keys;
    keys.reserve(result.size());

    for (size_t i = 0; i < result.size(); i++) {
        if (result[i][column_index]) {
            auto *data = dynamic_cast<Base *>(result[i][column_index].get());
            int id1 = static_cast<int>(static_cast<UIntData *>(data)->id);
            int id2 = static_cast<int>(static_cast<UBigIntData *>(data)->id);

            if (id1 == 30 || id2 == 30) {
                keys.push_back(static_cast<uint32_t>(static_cast<UIntData *>(data)->value));
            } else if (id1 == 31 || id2 == 31) {
                keys.push_back(static_cast<uint64_t>(static_cast<UBigIntData *>(data)->value));
            }
        }
    }

    // Ensure keys are sorted
    std::sort(keys.begin(), keys.end());

    // Build RadixSpline
    rs::Builder<T> builder(keys.front(), keys.back(), kNumRadixBits, kMaxError);
    int num = 0;
    for (const auto &key : keys) {
        builder.AddKey(key);
        num++;
    }

    auto finalized_spline = builder.Finalize();

    // Store in the appropriate map
    if constexpr (std::is_same<T, uint32_t>::value) {
        radix_spline_map_int32[map_key] = std::move(finalized_spline);
    } else if constexpr (std::is_same<T, uint64_t>::value) {
        radix_spline_map_int64[map_key] = std::move(finalized_spline);
    }

    std::cout << "RadixSpline successfully created for " << map_key << " Total Keys Added : "<< num << ".\n";
}


static QualifiedName GetQualifiedName(ClientContext &context, const string &qname_str) {
	auto qname = QualifiedName::Parse(qname_str);
	if (qname.schema == INVALID_SCHEMA) {
		qname.schema = ClientData::Get(context).catalog_search_path->GetDefaultSchema(qname.catalog);
	}
	return qname;
}

/**
 * PragmaFunction to Load the data
*/
void createRadixSplineIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();

    // Get the qualified name for the table
    QualifiedName qname = GetQualifiedName(context, table_name);

    // Get the table entry
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    auto &columnList = table.GetColumns();

    // Find the column index and type
    vector<string> columnNames = columnList.GetColumnNames();
    vector<LogicalType> columnTypes = columnList.GetColumnTypes();
    int column_index = -1;
    LogicalType column_type;

    for (size_t i = 0; i < columnNames.size(); i++) {
        if (columnNames[i] == column_name) {
            column_index = i;
            column_type = columnTypes[i];
            break;
        }
    }

    // Check if column was found
    if (column_index == -1) {
        std::cout << "Column '" << column_name << "' not found in table '" << table_name << "'.\n";
        return;
    }

    duckdb::Connection con(*context.db);

    // Determine column type and build RadixSpline
    string columnTypeName = column_type.ToString();
    if (columnTypeName == "UBIGINT") {
        BulkLoadRadixSpline<uint64_t>(con, qname.name, column_index, qname.catalog + "." + qname.schema + "." + qname.name + "." + column_name);
    } else if (columnTypeName == "UINTEGER") {
        BulkLoadRadixSpline<uint32_t>(con, qname.name, column_index, qname.catalog + "." + qname.schema + "." + qname.name + "." + column_name);
    } else {
        std::cout << "Unsupported column type '" << columnTypeName << "' for RadixSpline indexing.\n";
    }
}

/**
 * Function to lookup a value using the RadixSpline index
*/
void RadixSplineLookupPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();
    string lookup_key_str = parameters.values[2].GetValue<string>();

    // Parse the lookup key
    uint64_t lookup_key = std::stoull(lookup_key_str);

    std::cout <<lookup_key<<std::endl;

    // Create the map key
    QualifiedName qname = GetQualifiedName(context, table_name);
    string map_key = qname.catalog + "." + qname.schema + "." + qname.name + "." + column_name;

    // Determine which RadixSpline map to use
    if (radix_spline_map_int64.find(map_key) != radix_spline_map_int64.end()) {
        // Lookup in the uint64_t RadixSpline map
        const auto &radix_spline = radix_spline_map_int64[map_key];
        size_t estimated_position = radix_spline.GetEstimatedPosition(lookup_key);
        std::cout << "Estimated position for key " << lookup_key << " is: " << estimated_position << std::endl;
    } else if (radix_spline_map_int32.find(map_key) != radix_spline_map_int32.end()) {
        // Lookup in the uint32_t RadixSpline map
        uint32_t lookup_key_32 = static_cast<uint32_t>(lookup_key);
        const auto &radix_spline = radix_spline_map_int32[map_key];
        size_t estimated_position = radix_spline.GetEstimatedPosition(lookup_key_32);
        std::cout << "Estimated position for key " << lookup_key_32 << " is: " << estimated_position << std::endl;
    } else {
        std::cout << "RadixSpline index not found for " << map_key << ". Please ensure you have created the index first." << std::endl;
    }
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto radix_scalar_function = ScalarFunction("radix", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RadixScalarFun);
    ExtensionUtil::RegisterFunction(instance, radix_scalar_function);

    // Register another scalar function
    auto radix_openssl_version_scalar_function = ScalarFunction("radix_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, RadixOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, radix_openssl_version_scalar_function);

    // Register the create_radixspline_index pragma
    auto create_radixspline_index_function = PragmaFunction::PragmaCall(
        "create_radixspline_index",                             // Name of the pragma
        createRadixSplineIndexPragmaFunction,                  // Function to call
        {LogicalType::VARCHAR, LogicalType::VARCHAR},          // Expected argument types (table name, column name)
        {}
    );
    ExtensionUtil::RegisterFunction(instance, create_radixspline_index_function);

    // Register the lookup_radixspline pragma
    auto lookup_radixspline_function = PragmaFunction::PragmaCall(
        "lookup_radixspline",                                // Name of the pragma
        RadixSplineLookupPragmaFunction,                     // Function to call
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, // Expected argument types (table name, column name, lookup key)
        {}
    );
    ExtensionUtil::RegisterFunction(instance, lookup_radixspline_function);
}

void RadixExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string RadixExtension::Name() {
	return "radix";
}

std::string RadixExtension::Version() const {
#ifdef EXT_VERSION_RADIX
	return EXT_VERSION_RADIX;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void radix_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::RadixExtension>();
}

DUCKDB_EXTENSION_API const char *radix_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
