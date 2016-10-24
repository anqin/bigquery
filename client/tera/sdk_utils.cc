// Copyright (C) 2014, for authors.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:
//

#include "bigquery/client/tera/sdk_utils.h"

#include <stdarg.h>
#include <iomanip>
#include <iostream>

#include "common/base/string_ext.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

namespace bigquery {
namespace client {

using namespace ::tera;

string LgProp2Str(CompressType type) {
    if (type == kNoneCompress) {
        return "none";
    } else if (type == kSnappyCompress) {
        return "snappy";
    } else {
        return "";
    }
}

string LgProp2Str(StoreType type) {
    if (type == kInDisk) {
        return "disk";
    } else if (type == kInFlash) {
        return "flash";
    } else if (type == kInMemory) {
        return "memory";
    } else {
        return "";
    }
}

string TableProp2Str(RawKeyType type) {
    if (type == kReadable) {
        return "readable";
    } else if (type == kBinary) {
        return "binary";
    } else {
        return "";
    }
}

void ShowTableDescriptor(::tera::TableDescriptor& table_desc, std::ostream* stream) {
    *stream << "table schema: " << std::endl << std::endl;
    *stream << " " << table_desc.TableName()
        << " : {rawkey=" << TableProp2Str(table_desc.RawKey()) << "}" << std::endl;

    size_t lg_num = table_desc.LocalityGroupNum();
    size_t cf_num = table_desc.ColumnFamilyNum();
    for (size_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const ::tera::LocalityGroupDescriptor* lg_desc = table_desc.LocalityGroup(lg_no);
        *stream << " |---- " << lg_desc->Name()
            << " : {storage=" << LgProp2Str(lg_desc->Store())
            << ",compress=" << LgProp2Str(lg_desc->Compress())
            << ",blocksize=" << lg_desc->BlockSize()
             << "}" << std::endl;
        for (size_t cf_no = 0; cf_no < cf_num; ++cf_no) {
            const ::tera::ColumnFamilyDescriptor* cf_desc = table_desc.ColumnFamily(cf_no);
            if (cf_desc->LocalityGroup() != lg_desc->Name()) {
                continue;
            }

            if (lg_no < lg_num - 1) {
                *stream << " |     |---- ";
            } else {
                *stream << "       |---- ";
            }

            if (cf_desc->Name() == "") {
                *stream << "_(default cf)";
            } else {
                *stream << cf_desc->Name();
            }

            if (cf_desc->Type() != "") {
                *stream << "<" << cf_desc->Type() << ">";
            }
            *stream << " : {maxversions=" << cf_desc->MaxVersions()
                << ",minversions=" << cf_desc->MinVersions()
                << ",ttl=" << cf_desc->TimeToLive()
                << "}" << std::endl;
        }
    }
    *stream << std::endl;
    *stream << "Snapshot:" << std::endl;
    for (uint32_t i = 0; i < table_desc.SnapshotNum(); ++i) {
        *stream << " " << table_desc.Snapshot(i) << std::endl;
    }
}

bool CheckName(const string& name) {
    if (name.size() == 0) {
        return true;
    }
    if (name.length() >= 256) {
        LOG(ERROR) << name << " has too many chars: " << name.length();
        return false;
    }
    for (size_t i = 0; i < name.length(); ++i) {
        if (!(name[i] <= '9' && name[i] >= '0')
            && !(name[i] <= 'z' && name[i] >= 'a')
            && !(name[i] <= 'Z' && name[i] >= 'A')
            && !(name[i] == '_')
            && !(name[i] == '-')) {
            LOG(ERROR) << name << " has illegal chars \"" << name[i] << "\"";
            return false;
        }
    }
    if (name[0] <='9' && name[0] >= '0') {
        LOG(ERROR) << "name do not allow starting with digits: " << name[0];
        return false;
    }
    return true;
}

bool ParseCfNameType(const string& in, string* name, string* type) {
    int len = in.size();
    string name_t, type_t;
    if (name == NULL) {
        name = &name_t;
    }
    if (type == NULL) {
        type = &type_t;
    }
    if (len < 3 || in[len-1] != '>') {
        if (CheckName(in)) {
            *name = in;
            *type = "";
            return true;
        } else {
            LOG(ERROR) << "illegal cf name: " << in;
            return false;
        }
    }

    string::size_type pos = in.find('<');
    if (pos == string::npos) {
        LOG(ERROR) << "illegal cf name, need '<': " << in;
        return false;
    }
    *name = in.substr(0, pos);
    if (!CheckName(*name)) {
        LOG(ERROR) << "illegal cf name, need '<': " << in;
        return false;
    }
    *type = in.substr(pos + 1, len - pos - 2);
    return true;
}

// property syntax: name{prop1=value1[,prop=value]}
// name or property may be empty
// e.g. "lg0{disk,blocksize=4K}", "{disk}", "lg0"
bool ParseProperty(const string& schema, string* name, PropertyList* prop_list) {
    string::size_type pos_s = schema.find('{');
    string::size_type pos_e = schema.find('}');
    if (pos_s == string::npos && pos_e == string::npos) {
        // deal with non-property
        if (name) {
            *name = schema;
        }
        prop_list->clear();
        return true;
    }
    if (pos_s == string::npos || pos_e == string::npos) {
        LOG(ERROR) << "property should be included by \"{}\": " << schema;
        return false;
    }
    string tmp_name = schema.substr(0, pos_s);
    if (!CheckName(tmp_name)) {
        return false;
    }
    if (name) {
        *name = tmp_name;
    }
    if (prop_list == NULL) {
        return true;
    }
    string prop_str = schema.substr(pos_s + 1, pos_e - pos_s -1);
    std::vector<string> props;
    SplitString(prop_str, ",", &props);
    prop_list->clear();
    for (size_t i = 0; i < props.size(); ++i) {
        std::vector<string> p;
        SplitString(props[i], "=", &p);
        if (p.size() == 1) {
            prop_list->push_back(std::make_pair(p[0], ""));
        } else if (p.size() == 2) {
            prop_list->push_back(std::make_pair(p[0], p[1]));
        } else {
            return false;
        }
        if (!CheckName((*prop_list)[i].first)) {
            return false;
        }
    }
    return true;
}

bool SetCfProperties(const PropertyList& props, ::tera::ColumnFamilyDescriptor* desc) {
    for (size_t i = 0; i < props.size(); ++i) {
        const Property& prop = props[i];
        if (prop.first == "ttl") {
            int64_t ttl = atol(prop.second.c_str());
            desc->SetTimeToLive(ttl);
        } else if (prop.first == "maxversions") {
            int32_t versions = atol(prop.second.c_str());
            desc->SetMaxVersions(versions);
        } else if (prop.first == "minversions") {
            int32_t versions = atol(prop.second.c_str());
            desc->SetMinVersions(versions);
        } else if (prop.first == "diskquota") {
            int64_t quota = atol(prop.second.c_str());
            desc->SetDiskQuota(quota);
        } else {
            LOG(ERROR) << "illegal cf props: " << prop.first;
            return false;
        }
    }
    return true;
}

bool SetLgProperties(const PropertyList& props, ::tera::LocalityGroupDescriptor* desc) {
    for (size_t i = 0; i < props.size(); ++i) {
        const Property& prop = props[i];
        if (prop.first == "compress") {
            if (prop.second == "none") {
                desc->SetCompress(kNoneCompress);
            } else if (prop.second == "snappy") {
                desc->SetCompress(kSnappyCompress);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                    << "for property: " << prop.first;
                return false;
            }
        } else if (prop.first == "storage") {
            if (prop.second == "disk") {
                desc->SetStore(kInDisk);
            } else if (prop.second == "flash") {
                desc->SetStore(kInFlash);
            } else if (prop.second == "memory") {
                desc->SetStore(kInMemory);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                    << "for property: " << prop.first;
                return false;
            }
        } else if (prop.first == "blocksize") {
            int blocksize = atoi(prop.second.c_str());
            desc->SetBlockSize(blocksize);
        } else {
            LOG(ERROR) << "illegal lg property: " << prop.first;
            return false;
        }
    }
    return true;
}

bool SetTableProperties(const PropertyList& props, ::tera::TableDescriptor* desc) {
    for (size_t i = 0; i < props.size(); ++i) {
        const Property& prop = props[i];
        if (prop.first == "rawkey") {
            if (prop.second == "readable") {
                desc->SetRawKey(kReadable);
            } else if (prop.second == "binary") {
                desc->SetRawKey(kBinary);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                    << "for property: " << prop.first;
                return false;
            }
        } else {
            LOG(ERROR) << "illegal table property: " << prop.first;
            return false;
        }
    }
    return true;
}

bool ParseCfSchema(const string& schema,
                   ::tera::TableDescriptor* table_desc,
                   ::tera::LocalityGroupDescriptor* lg_desc) {
    string name_type, cf_name, cf_type;
    PropertyList cf_props;
    if (!ParseProperty(schema, &name_type, &cf_props)) {
        return false;
    }
    if (!ParseCfNameType(name_type, &cf_name, &cf_type)) {
        return false;
    }

    ::tera::ColumnFamilyDescriptor* cf_desc;
    cf_desc = table_desc->AddColumnFamily(cf_name, lg_desc->Name());
    if (cf_desc == NULL) {
        LOG(ERROR) << "fail to add column family: " << cf_name;
        return false;
    }
    cf_desc->SetType(cf_type);

    if (!SetCfProperties(cf_props, cf_desc)) {
        LOG(ERROR) << "fail to set cf properties: " << cf_name;
        return false;
    }
    return true;
}

bool CommaInBracket(const string& full, string::size_type pos) {
    string::size_type l_pos = 0;
    string::size_type r_pos = string::npos;

    l_pos = full.find_last_of("{", pos);
    r_pos = full.find_first_of("}", pos);
    if (l_pos == string::npos || r_pos == string::npos) {
        return false;
    }
    if (r_pos > full.find_first_of("}", l_pos)) {
        return false;
    }
    if (l_pos < full.find_last_of("{", r_pos)) {
        return false;
    }
    return true;
}

void SplitCfSchema(const string& full, std::vector<string>* result) {
    const string delim = ",";
    result->clear();
    if (full.empty()) {
        return;
    }

    string tmp;
    string::size_type pos_begin = full.find_first_not_of(delim);
    string::size_type comma_pos = pos_begin;

    while (pos_begin != string::npos) {
        do {
            comma_pos = full.find(delim, comma_pos + 1);
        } while (CommaInBracket(full, comma_pos));

        if (comma_pos != string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result->push_back(tmp);
            tmp.clear();
        }
    }
}

bool ParseLgSchema(const string& schema, ::tera::TableDescriptor* table_desc) {
    std::vector<string> parts;
    SplitString(schema, ":", &parts);
    if (parts.size() != 2) {
        LOG(ERROR) << "lg syntax error: " << schema;
        return false;
    }

    string lg_name;
    PropertyList lg_props;
    if (!ParseProperty(parts[0], &lg_name, &lg_props)) {
        return false;
    }

    ::tera::LocalityGroupDescriptor* lg_desc;
    lg_desc = table_desc->AddLocalityGroup(lg_name);
    if (lg_desc == NULL) {
        LOG(ERROR) << "fail to add locality group: " << lg_name;
        return false;
    }

    if (!SetLgProperties(lg_props, lg_desc)) {
        LOG(ERROR) << "fail to set lg properties: " << lg_name;
        return false;
    }

    std::vector<string> cfs;
    SplitCfSchema(parts[1], &cfs);
    CHECK(cfs.size() > 0);
    for (size_t i = 0; i < cfs.size(); ++i) {
        if (!ParseCfSchema(cfs[i], table_desc, lg_desc)) {
            return false;
        }
    }
    return true;
}

bool ParseSchema(const string& schema, ::tera::TableDescriptor* table_desc) {
    std::vector<string> parts;
    SplitString(schema, "#", &parts);
    string lg_props;

    if (parts.size() == 2) {
        PropertyList props;
        if (!ParseProperty(parts[0], NULL, &props)) {
            return false;
        }
        if (!SetTableProperties(props, table_desc)) {
            return false;
        }
        lg_props = parts[1];
    } else {
        lg_props = parts[0];
    }

    std::vector<string> lgs;
    SplitString(lg_props, "|", &lgs);

    for (size_t i = 0; i < lgs.size(); ++i) {
        if (!ParseLgSchema(lgs[i], table_desc)) {
            return false;
        }
    }

    return true;
}

bool ParseKvSchema(const string& schema, ::tera::LocalityGroupDescriptor* lg_desc) {
    PropertyList lg_props;
    if (!ParseProperty(schema, NULL, &lg_props)) {
        return false;
    }

    if (!SetLgProperties(lg_props, lg_desc)) {
        return false;
    }
    return true;
}

bool BuildSchema(::tera::TableDescriptor* table_desc, string* schema) {
    // build schema string from table descriptor
    if (schema == NULL) {
        LOG(ERROR) << "schema string is NULL.";
        return false;
    }
    if (table_desc == NULL) {
        LOG(ERROR) << "table descriptor is NULL.";
        return false;
    }

    schema->clear();
    int lg_num = table_desc->LocalityGroupNum();
    int cf_num = table_desc->ColumnFamilyNum();
    for (size_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const ::tera::LocalityGroupDescriptor* lg_desc = table_desc->LocalityGroup(lg_no);
        string lg_name = lg_desc->Name();
        if (lg_no > 0) {
            schema->append("|");
        }
        schema->append(lg_name);
        schema->append(":");
        int cf_cnt = 0;
        for (size_t cf_no = 0; cf_no < cf_num; ++cf_no) {
            const ::tera::ColumnFamilyDescriptor* cf_desc = table_desc->ColumnFamily(cf_no);
            if (cf_desc->LocalityGroup() == lg_name && cf_desc->Name() != "") {
                if (cf_cnt++ > 0) {
                    schema->append(",");
                }
                schema->append(cf_desc->Name());
            }
        }
    }
    return true;
}

class CliPrinter::Impl {
public:
    typedef std::vector<string> Line;
    typedef std::vector<Line> Table;

    Impl(int cols) : _cols(cols) {
        if (cols > 0) {
            _col_width.resize(cols, 0);
        }
    }

    bool AddRow(int argc, va_list args) {
        if (argc != _cols) {
            LOG(ERROR) << "arg num error: " << argc << " vs " << _cols;
            return false;
        }
        Line line;
        for (int i = 0; i < argc; ++i) {
            string item = va_arg(args, char*);
            if (item.size() > _col_width[i]) {
                _col_width[i] = item.size();
            }
            if (item.size() == 0) {
                item = "-";
            }
            line.push_back(item);
        }
        _table.push_back(line);
        return true;
    }

    void Print(std::ostream* stream) {
        if (_table.size() < 1) {
            return;
        }
        int line_len = 0;
        for (int i = 0; i < _cols; ++i) {
            line_len += 2 + _col_width[i];
            *stream << "  " << std::setfill(' ')
                << std::setw(_col_width[i])
                << std::setiosflags(std::ios::left)
                << _table[0][i];
        }
        *stream << std::endl;
        for (int i = 0; i < line_len + 2; ++i) {
            *stream << "-";
        }
        *stream << std::endl;

        for (int i = 1; i < _table.size(); ++i) {
            for (int j = 0; j < _cols; ++j) {
                *stream << "  " << std::setfill(' ')
                    << std::setw(_col_width[j])
                    << std::setiosflags(std::ios::left)
                    << _table[i][j];
            }
            *stream << std::endl;
        }
    }

    void Reset() {
        _col_width.resize(_cols, 0);
        _table.clear();
    }
private:
    int _cols;
    std::vector<int> _col_width;
    Table _table;
};

CliPrinter::CliPrinter(int cols) {
    _impl = new Impl(cols);
}

CliPrinter::~CliPrinter() {
    delete _impl;
}

bool CliPrinter::AddRow(int argc, ...) {
    va_list args;
    va_start(args, argc);
    bool retval = _impl->AddRow(argc, args);
    va_end(args);
    return retval;
}

void CliPrinter::Print(std::ostream* stream) {
    _impl->Print(stream);
}

//////////// print function //////////

std::string ConvertByteToString(const uint64_t size) {
    std::string hight_unit;
    double min_size;
    const uint64_t kKB = 1024;
    const uint64_t kMB = kKB * 1024;
    const uint64_t kGB = kMB * 1024;
    const uint64_t kTB = kGB * 1024;
    const uint64_t kPB = kTB * 1024;

    if (size == 0) {
        return "0 ";
    }

    if (size > kPB) {
        min_size = (1.0 * size) / kPB;
        hight_unit = "P";
    } else if (size > kTB) {
        min_size = (1.0 * size) / kTB;
        hight_unit = "T";
    } else if (size > kGB) {
        min_size = (1.0 * size) / kGB;
        hight_unit = "G";
    } else if (size > kMB) {
        min_size = (1.0 * size) / kMB;
        hight_unit = "M";
    } else if (size > kKB) {
        min_size = (1.0 * size) / kKB;
        hight_unit = "K";
    } else {
        min_size = size;
        hight_unit = " ";
    }

    if ((int)min_size - min_size == 0) {
        return StringFormat("%d%s", (int)min_size, hight_unit.c_str());
    } else {
        return StringFormat("%.2f%s", min_size, hight_unit.c_str());
    }
}


} // namespace client
} // namespace bigquery
