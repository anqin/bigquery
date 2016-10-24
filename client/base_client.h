// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef BIGQUERY_CLIENT_BASE_CLIENT_H
#define BIGQUERY_CLIENT_BASE_CLIENT_H

#include <ostream>
#include <string>
#include <vector>

#include "common/base/scoped_ptr.h"

#include "bigquery/parser/big_query_parser.h"
#include "bigquery/parser/big_query_types.h"
#include "bigquery/parser/select_stmt.pb.h"
#include "bigquery/parser/table_schema.h"

#define PROMPT_GREEN_BEGIN  "\033[32m"
#define PROMPT_RED_BEGIN  "\033[31m"
#define PROMPT_COLOR_END    "\033[0m"

namespace bigquery {
namespace client {

class BaseClient {
public:
    BaseClient();

    virtual ~BaseClient() {}

    bool Run(const std::string& query);

    bool Run(const std::string& query, std::ostream* out);

protected:
    bool Run(const std::string& query, const std::string& job_identity,
             std::ostream* out);

    virtual bool RunQueryTable(const compiler::QueryStmt& stmt,
                               const std::string& query,
                               const std::string& job_identity,
                               std::ostream* stream);

    virtual void RunQuit(std::ostream* stream);

    virtual bool RunCreateTable(const compiler::CreateTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream);

    virtual bool RunDefineTable(const compiler::DefineTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream);

    virtual bool RunDropTable(const compiler::DropTableStmt& stmt,
                              const std::string& job_identity,
                              std::ostream* stream);

    bool RunHelp(const compiler::HelpStmt& stmt,
                 std::ostream* stream);

    virtual bool RunShow(const compiler::ShowStmt& stmt,
                         const std::string& job_identity,
                         std::ostream* stream);

    virtual bool DropTable(const std::string& table_name,
                           const std::string& job_identity);

//     virtual void AssembleJobResult(uint64_t job_id,
//                                    const GetJobResultResponse& response,
//                                    std::ostream* stream);
//     virtual void DumpHistoryQuery(const GetHistoryQueryResponse& request,
//                                   std::ostream* stream);

    virtual std::string GreenString(const std::string& word) {
        return PROMPT_GREEN_BEGIN + word + PROMPT_COLOR_END;
    }

    virtual std::string RedString(const std::string& word) {
        return PROMPT_RED_BEGIN + word + PROMPT_COLOR_END;
    }
};

} // namespace client
} // namespace bigquery

#endif // BIGQUERY_CLIENT_BASE_CLIENT_H
