// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  BIGQUERY_CLIENT_SHELL_H
#define  BIGQUERY_CLIENT_SHELL_H

#include <string>


namespace bigquery {
namespace client {
class BaseClient;
}  // namespace client

char** CompletionCmd(const char *text, int start, int end);

class QueryShell {
public:
    QueryShell();
    virtual ~QueryShell();

    void Init(client::BaseClient* client);

    int Run();

    int Run(const char* read_line);

private:
    size_t GetHyphenIndex(const std::string& cur_line);

    void InitializeReadLine();

private:
    client::BaseClient* m_client;
    std::string m_head_name;
    std::string m_read_line;
};

} // namespace bigquery

#endif  // BIGQUERY_CLIENT_SHELL_H
