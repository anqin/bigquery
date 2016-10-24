// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef BIGQUERY_CLIENT_TERA_CLEINT_H
#define BIGQUERY_CLIENT_TERA_CLEINT_H

#include "bigquery/client/base_client.h"

#include "tera.h"

namespace bigquery {
namespace client {

class TeraClient : public BaseClient {
public:
    TeraClient(::tera::Client* client);
    virtual ~TeraClient();

protected:
    bool RunShow(const compiler::ShowStmt& stmt,
                 const std::string& job_identity,
                 std::ostream* stream);

    bool RunQueryTable(const compiler::QueryStmt& stmt,
                       const std::string& query,
                       const std::string& job_identity,
                       std::ostream* stream);

private:
    bool ShowAllTables(std::ostream* stream);
    bool ShowSingleTable(const std::string& table_name, std::ostream* stream);
    bool PrintTabletList(std::vector< ::tera::TabletInfo>& tablet_list, bool include_name,
                         std::ostream* stream);

    ::tera::Table* GetTableHandle(const std::string& table_name);
    bool QuerySingleTable(const compiler::SelectStmt& stmt,
                          const std::string& query,
                          const std::string& table_name,
                          std::ostream* stream);

    bool GetTableSchema(const std::string& table_name,
                        std::map<std::string, std::string>* field_types);

private:
    ::tera::Client* m_client;

    std::map<std::string, ::tera::Table*> m_tables;
};

} // namespace client
} // namespace bigquery

#endif // BIGQUERY_CLIENT_TERA_CLEINT_H
