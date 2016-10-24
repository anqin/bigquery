// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef BIGQUERY_CLIENT_ROW_SCANNER_H
#define BIGQUERY_CLIENT_ROW_SCANNER_H

#include <ostream>

#include "tera.h"

#include "bigquery/parser/analyzer.h"
#include "bigquery/parser/select_stmt.pb.h"

namespace bigquery {
namespace client {

class RowScanner {
public:
    RowScanner(compiler::Analyzer* analyzer,
               ::tera::Table* table,
               const std::map<std::string, std::string>& field_type);
    ~RowScanner();

    bool Scan(const ::tera::ScanDescriptor& desc);
    bool Current(std::ostream* stream);
    bool Next();

private:
    void OutputRecordString(const std::string& family, const std::string& value,
                            std::ostream* stream);

private:
    compiler::Analyzer* m_analyzer;
    ::tera::Table* m_table;
    const std::map<std::string, std::string>& m_field_type;

    std::string m_cur_row_key;
    ::tera::ResultStream* m_scan_stream;
};

} // namespace client
} // namespace bigquery

#endif // BIGQUERY_CLIENT_ROW_SCANNER_H
