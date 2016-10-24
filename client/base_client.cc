// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "bigquery/client/base_client.h"

#include <signal.h>

#include <vector>

#include "common/base/scoped_ptr.h"

#include "bigquery/client/sql_command_info.h"
#include "bigquery/parser/column_metadata.pb.h"


namespace bigquery {
namespace client {
BaseClient::BaseClient() {
}

bool BaseClient::Run(const std::string& query) {
    return Run(query, "", &(std::cout));
}

bool BaseClient::Run(const std::string& query, std::ostream* out) {
    return Run(query, "", out);
}

bool BaseClient::Run(const std::string& query,
                     const std::string& job_identity,
                     std::ostream* out) {
    scoped_ptr<compiler::QueryStmt> query_stmt;
    compiler::QueryStmt* tmp_stmt = NULL;
    int flag = parse_line(query.c_str(), &tmp_stmt, out);
    query_stmt.reset(tmp_stmt);

    if (flag != 0) {
        return false;
    }

    bool ret = true;
    if (query_stmt->has_select()) {
        ret =  RunQueryTable(*(query_stmt.get()), query, job_identity, out);
    } else if (query_stmt->has_create()) {
        ret = RunCreateTable(query_stmt->create(), job_identity, out);
    } else if (query_stmt->has_define()) {
        ret = RunDefineTable(query_stmt->define(), job_identity, out);
    } else if (query_stmt->has_drop()) {
        ret = RunDropTable(query_stmt->drop(), job_identity, out);
    } else if (query_stmt->has_help()) {
        ret = RunHelp(query_stmt->help(), out);
    } else if (query_stmt->has_quit()) {
        RunQuit(out);
        ret = true;
    } else if (query_stmt->has_show()) {
        ret = RunShow(query_stmt->show(), job_identity, out);
    } else {
        LOG(ERROR) << " Get empty query stmt here ";
    }

    return ret;
}

// void BaseClient::GetQueryTables(const compiler::SelectStmt& stmt,
//                                 std::vector<std::string>* table_names) const {
// }

bool BaseClient::RunQueryTable(const compiler::QueryStmt& stmt,
                               const std::string& query,
                               const std::string& job_identity,
                               std::ostream* stream) {
    LOG(INFO) << "RunQueryTable()";
    return true;
}

bool BaseClient::RunCreateTable(const compiler::CreateTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream) {
    return true;
}

bool BaseClient::RunDefineTable(const compiler::DefineTableStmt& stmt,
                                const std::string& job_identity,
                                std::ostream* stream) {
    return true;
}

bool BaseClient::RunDropTable(const compiler::DropTableStmt& stmt,
                              const std::string& job_identity,
                              std::ostream* stream) {
    return true;
}

bool BaseClient::DropTable(const std::string& table_name,
                           const std::string& job_identity) {
    return true;
}

void BaseClient::RunQuit(std::ostream* stream) {
    std::cout << GreenString("You have terminated the tera shell ... bye")
        << std::endl;
    raise(SIGINT);
}

bool BaseClient::RunShow(const compiler::ShowStmt& stmt,
                         const std::string& job_identity,
                         std::ostream* stream) {
    return true;
}

// void BaseClient::DumpHistoryQuery(const GetHistoryQueryResponse& response,
//                                   std::ostream* stream) {
// }

bool BaseClient::RunHelp(const compiler::HelpStmt& stmt,
                         std::ostream* stream) {
    int index = 0;
    if (stmt.has_cmd_name()) {
        const char *name = NULL;
        const char *cmd_name = stmt.cmd_name().char_string().c_str();
        int len = strlen(cmd_name);
        while (NULL != (name = g_command_lists[index].command)) {
            if (0 == strncmp(name, cmd_name, len)) {
                *stream << GreenString(g_command_lists[index].command)
                    << ":" << g_command_lists[index].descr << std::endl
                    << "Usage:" << std::endl
                    << g_command_lists[index].usage << std::endl;
                break;
            }
            index++;
        }
        if (name == NULL) {
            LOG(ERROR) << "not found command: "
                << stmt.cmd_name().char_string();
            return false;
        }
    } else {
        while (NULL != g_command_lists[index].command) {
            *stream << GreenString(g_command_lists[index].command)
                << "\t\t" << g_command_lists[index].descr
                << std::endl;
            index++;
        }
    }
    return true;
}

#if 0
static uint64_t kMaxPrintSize = 512;
void BaseClient::AssembleJobResult(uint64_t job_id,
                                   const GetJobResultResponse& response,
                                   std::ostream* stream) {
    std::string file_name = GetUserJobResultFileName(job_id, "text");
    scoped_ptr<File> file(File::Open(file_name.c_str(),
                                     File::ENUM_FILE_OPEN_MODE_W));
    CHECK_NOTNULL(file.get());

    MemPool mempool(MemPool::MAX_UNIT_SIZE);
    uint64_t print_size = 0;
    bool is_print_last_line = false;
    const std::string comment = GreenString("-----------------");
    for (int32_t i = 0; i < response.result().job_result_tablets_size(); ++i) {
        io::TabletReader reader(&mempool);
        CHECK(reader.Init(response.result().job_result_tablets(i)));

        io::PbRecordAssembler assembler;
        CHECK(assembler.Init(&reader));

        scoped_ptr<protobuf::Message> message;
        message.reset(assembler.GetProtoMessage()->New());
        std::string record = "";
        while (assembler.AssembleRecord(message.get())) {
            record = message->Utf8DebugString();
            uint32_t length = record.length();
            if (print_size < kMaxPrintSize) {
                *stream << comment + GreenString(" Record ") + comment
                    << std::endl << record;
                print_size += length;
            } else if (!is_print_last_line) {
                *stream << comment + GreenString(" More record in file : ")
                    << RedString(file_name) + " " + comment << std::endl;
                is_print_last_line = true;
            }
            if (length > 0) {
                CHECK_NE(-1, file->Write(record.c_str(), length))
                    << "Write file error : " << file_name;
            }
        }
        reader.Close();
    }

    if (!is_print_last_line) {
        *stream << comment + GreenString(" All record are saved in file : ")
            << RedString(file_name) + " " + comment << std::endl;
    }

    *stream << comment << GreenString(" Time consume : ")
        << GetJobRunningTime(response.result().job_timestamp())
        << GreenString("(s) ") << ", Query result size : "
        << CovertByteToString(response.result().result_file_size())
        << comment << std::endl;

    CHECK_NE(-1, file->Close()) << "Close output file error ";
}
#endif

} // namespace client
} // namespace bigquery
