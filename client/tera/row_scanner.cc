// Copyright (C) 2014, for authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "bigquery/client/tera/row_scanner.h"

#include <sstream>

namespace bigquery {
namespace client {


RowScanner::RowScanner(compiler::Analyzer* analyzer,
                       ::tera::Table* table,
                       const std::map<std::string, std::string>& field_type)
    : m_analyzer(analyzer), m_table(table), m_field_type(field_type),
      m_cur_row_key(""), m_scan_stream(NULL) {}

RowScanner::~RowScanner() {}

bool RowScanner::Scan(const ::tera::ScanDescriptor& desc) {
    LOG(INFO) << "RowScanner::Scan()";
    ::tera::ErrorCode err;
    m_scan_stream = m_table->Scan(desc, &err);
    if (m_scan_stream == NULL) {
        LOG(ERROR) << "fail to scan table, status: "
            << err.GetReason();
        return false;
    }
    m_cur_row_key = m_scan_stream->RowName();
    return true;
}

bool RowScanner::Current(std::ostream* stream) {
    bool is_scan_cell = true;
    std::stringstream os;

    m_analyzer->Reset();
    while (is_scan_cell) {
        if (m_cur_row_key.empty() || m_scan_stream->RowName() == m_cur_row_key) {
            if (m_cur_row_key.empty()) {
                m_cur_row_key = m_scan_stream->RowName();
            }
            // check filter & save data
            if (m_analyzer->CheckField(m_scan_stream->Family(), m_scan_stream->Value())) {
                LOG(INFO) << "found: " << m_scan_stream->Family();
            }

            if (m_analyzer->IsTarget(m_scan_stream->Family())) {
                os << "| ";
                OutputRecordString(m_scan_stream->Family(), m_scan_stream->Value(), &os);
            }
            m_scan_stream->Next();
            if (m_scan_stream->Done()) {
                is_scan_cell = false;
            }

        } else {
            is_scan_cell = false;
        }
    }
    if (m_analyzer->IsOk()) {
        *stream << os.str() << "|"  << std::endl;
        return true;
    }
    return false;
}

bool RowScanner::Next() {
    bool is_stop = false;
    while (!is_stop) {
        if (m_scan_stream->RowName() != m_cur_row_key) {
            m_cur_row_key = m_scan_stream->RowName();
            is_stop = true;
            continue;
        }
        m_scan_stream->Next();
        if (m_scan_stream->Done()) {
            LOG(WARNING) << "not more record";
            return false;
        }
    }
    return true;
}

void RowScanner::OutputRecordString(const std::string& field,
                                    const std::string& value,
                                    std::ostream* stream) {
    *stream << field << ":";
    std::map<std::string, std::string>::const_iterator it = m_field_type.find(field);
    if (it == m_field_type.end()) {
        return;
    }

    if (it->second == "uint64") {
        uint64_t data = 0;
        memcpy(&data, value.c_str(), sizeof(uint64_t));
        *stream << data;
    } else {
        *stream << value;
    }
    *stream << " ";
}

} // namespace client
} // namespace bigquery
