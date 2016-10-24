// Copyright (C) 2014, for authors.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:
//

#ifndef  BIGQUERY_CLIENT_SDK_UTILS_H
#define  BIGQUERY_CLIENT_SDK_UTILS_H

#include <iostream>
#include <ostream>

#include "tera.h"

using std::string;

namespace bigquery {
namespace client {

void ShowTableDescriptor(::tera::TableDescriptor& table_desc, std::ostream* stream = &std::cout);

// length<256 && only-allow-chars-digits-'_' && not-allow-starting-with-digits
bool CheckName(const string& name);

// extract cf name and type from schema string
// e.g. schema string: cf_link<int>
//      cf name      : cf_link
//      cf type      : int
bool ParseCfNameType(const string& in, string* name, string* type);

bool ParseSchema(const string& schema, ::tera::TableDescriptor* table_desc);

bool ParseKvSchema(const string& schema, ::tera::LocalityGroupDescriptor* lg_desc);

typedef std::pair<string, string> Property;
typedef std::vector<Property> PropertyList;
bool ParseProperty(const string& schema, string* name, PropertyList* prop_list);

bool SetCfProperties(const PropertyList& props, ::tera::ColumnFamilyDescriptor* desc);

bool SetLgProperties(const PropertyList& props, ::tera::LocalityGroupDescriptor* desc);

bool SetTableProperties(const PropertyList& props, ::tera::TableDescriptor* desc);

bool ParseCfSchema(const string& schema, ::tera::TableDescriptor* table_desc, ::tera::LocalityGroupDescriptor* lg_desc);

bool ParseLgSchema(const string& schema, ::tera::TableDescriptor* table_desc);

bool BuildSchema(::tera::TableDescriptor* table_desc, string* schema);

bool CommaInBracket(const string& full, string::size_type pos);

void SplitCfSchema(const string& full, std::vector<string>* result);

class CliPrinter {
public:
    CliPrinter(int cols);
    ~CliPrinter();
    bool AddRow(int argc, ...);
    void Print(std::ostream* stream = &std::cout);

private:
    class Impl;
    CliPrinter();
    Impl* _impl;
};

std::string ConvertByteToString(const uint64_t size);

} // namespace client
} // namespace bigquery
#endif // BIGQUERY_CLIENT_SDK_UTILS_H
