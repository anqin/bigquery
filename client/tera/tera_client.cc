// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "bigquery/client/tera/tera_client.h"

#include "common/base/string_number.h"
#include "tera.h"
#include "thirdparty/glog/logging.h"

#include "bigquery/client/tera/row_scanner.h"
#include "bigquery/client/tera/sdk_utils.h"

namespace bigquery {
namespace client {

TeraClient::TeraClient(::tera::Client* client)
    : m_client(client) {}

TeraClient::~TeraClient() {
    std::map<std::string, ::tera::Table*>::iterator it = m_tables.begin();
    for (; it != m_tables.end(); ++it) {
        delete it->second;
    }
}

bool TeraClient::RunShow(const compiler::ShowStmt& stmt,
                         const std::string& job_identity,
                         std::ostream* stream) {
    // first, should check identity
    if (stmt.has_table_name()) {
        return ShowSingleTable(stmt.table_name().char_string(), stream);
    } else {
        return ShowAllTables(stream);
    }
}

bool TeraClient::RunQueryTable(const compiler::QueryStmt& stmt,
                               const std::string& query,
                               const std::string& job_identity,
                               std::ostream* stream) {
    LOG(INFO) << "stmt = {" << stmt.DebugString() << "}, "
        << "query = " << query << std::endl;
    const compiler::RawTableList& table_list = stmt.select().from_list();
    for (int32_t i = 0; i < table_list.table_list_size(); ++i) {
        QuerySingleTable(stmt.select(), query, table_list.table_list(i)
                         .table_name().char_string(), stream);
    }
    return false;
}

bool TeraClient::ShowAllTables(std::ostream* stream) {
    std::vector< ::tera::TableInfo> table_list;
    std::vector< ::tera::TabletInfo> tablet_list;
    ::tera::ErrorCode err;
    if (!m_client->List(&table_list, &tablet_list, &err)) {
        LOG(ERROR) << "fail to get meta data from tera, status: "
            << err.GetReason();
        return false;
    }

    CliPrinter printer(5);
    printer.AddRow(5, " ", "table name", "status", "table size", "tablet num");
    for (size_t table_no = 0; table_no < table_list.size(); ++table_no) {
        std::string tablename = table_list[table_no].table_desc->TableName();
        std::string status = table_list[table_no].status;
        int32_t tablet_num = 0;
        int64_t table_size = 0;
        for (size_t i = 0; i < tablet_list.size(); ++i) {
            if (tablet_list[i].table_name == tablename) {
                tablet_num++;
                table_size += tablet_list[i].data_size;
            }
        }
        printer.AddRow(5,
                       NumberToString(table_no).data(),
                       tablename.data(),
                       status.data(),
                       ConvertByteToString(table_size).data(),
                       NumberToString(tablet_num).data());
    }
    printer.Print(stream);
    *stream << std::endl;
    for (size_t table_no = 0; table_no < table_list.size(); ++table_no) {
        delete table_list[table_no].table_desc;
    }
    return true;

}

bool TeraClient::ShowSingleTable(const std::string& table_name, std::ostream* stream) {
    ::tera::TableInfo table_info = {NULL, ""};
    std::vector< ::tera::TabletInfo> tablet_list;

    ::tera::ErrorCode err;
    if (!m_client->List(table_name, &table_info, &tablet_list, &err)) {
        LOG(ERROR) << "fail to get meta data from tera, status: "
            << err.GetReason();
        return false;
    }

    if (table_info.table_desc == NULL) {
        return false;
    }
    ShowTableDescriptor(*table_info.table_desc, stream);
    delete table_info.table_desc;

    PrintTabletList(tablet_list, false, stream);
    *stream << std::endl;

    return true;
}

bool TeraClient::PrintTabletList(std::vector< ::tera::TabletInfo>& tablet_list, bool include_name,
                                 std::ostream* stream) {
    if (include_name) {
        CliPrinter printer(8);
        printer.AddRow(8, " ", "table name", "address", "path", "size",
                       "status", "startkey", "endkey");
        for (size_t i = 0; i < tablet_list.size(); ++i) {
            printer.AddRow(8,
                           NumberToString(i).data(),
                           tablet_list[i].table_name.data(),
                           tablet_list[i].server_addr.data(),
                           tablet_list[i].path.data(),
                           ConvertByteToString(tablet_list[i].data_size).data(),
                           tablet_list[i].status.data(),
                           tablet_list[i].start_key.data(),
                           tablet_list[i].end_key.data());
        }
        printer.Print();
    } else {
        printf("\ntablet info:\n");
        CliPrinter printer(7);
        printer.AddRow(7, " ", "address", "path", "size", "status", "startkey", "endkey");
        for (size_t i = 0; i < tablet_list.size(); ++i) {
            printer.AddRow(7,
                           NumberToString(i).data(),
                           tablet_list[i].server_addr.data(),
                           tablet_list[i].path.data(),
                           ConvertByteToString(tablet_list[i].data_size).data(),
                           tablet_list[i].status.data(),
                           tablet_list[i].start_key.data(),
                           tablet_list[i].end_key.data());
        }
        printer.Print(stream);
    }
    return true;
}

::tera::Table* TeraClient::GetTableHandle(const std::string& table_name) {
    std::map<std::string, ::tera::Table*>::iterator it = m_tables.find(table_name);
    if (it != m_tables.end()) {
        return it->second;
    }

    LOG(WARNING) << "'" << table_name << "' not opened, open it now";
    ::tera::ErrorCode err;
    if (!m_client->IsTableExist(table_name, &err)) {
        LOG(ERROR) << "table '" << table_name << "' not exist";
        return NULL;
    }

    ::tera::Table* table = m_client->OpenTable(table_name, &err);
    if (table == NULL) {
        LOG(ERROR) << "fail to open table: " << table_name
            << ", status: " << err.GetReason();
        return NULL;
    }
    m_tables[table_name] = table;
    return table;
}

bool TeraClient::QuerySingleTable(const compiler::SelectStmt& stmt,
                                  const std::string& query,
                                  const std::string& table_name,
                                  std::ostream* stream) {
    ::tera::Table* table = GetTableHandle(table_name);
    if (table == NULL) {
        return false;
    }

    ::tera::ScanDescriptor scan_desc("");
    for (int32_t i = 0; i < stmt.target_list().target_list_size(); ++i) {
        scan_desc.AddColumnFamily(stmt.target_list().target_list(i)
                                  .expression().atomic().column()
                                  .field_list(0).char_string());
    }

    compiler::Analyzer analyzer(stmt);
    std::vector<std::string> cond_field_list;
    analyzer.GetFieldFromWhereClause(&cond_field_list);
    for (uint32_t i = 0; i < cond_field_list.size(); ++i) {
        scan_desc.AddColumnFamily(cond_field_list[i]);
    }

    std::map<std::string, std::string> field_type_list;
    GetTableSchema(table_name, &field_type_list);
    RowScanner scanner(&analyzer, table, field_type_list);
    if (!scanner.Scan(scan_desc)) {
        LOG(ERROR) << "fail to scan the start location";
        return false;
    }

    int64_t count = 0;
    do {
        if (scanner.Current(stream)) {
            count++;
        }
    } while (scanner.Next() && count < analyzer.MaxLimit());

    return true;
}

bool TeraClient::GetTableSchema(const std::string& table_name,
                                std::map<std::string, std::string>* field_types) {
    ::tera::ErrorCode err;
    ::tera::TableDescriptor* table_desc = m_client->GetTableDescriptor(table_name, &err);
    if (table_desc == NULL) {
        LOG(ERROR) << "fail to get table descriptor for: " << table_name
            << ", status: " << err.GetReason();
        return false;
    }

    for (int32_t i = 0; i < table_desc->ColumnFamilyNum(); ++i) {
        const ::tera::ColumnFamilyDescriptor* cf_desc =  table_desc->ColumnFamily(i);
        (*field_types)[cf_desc->Name()] = cf_desc->Type();
    }
    return true;
}

} // namespace client
} // namespace bigquery
