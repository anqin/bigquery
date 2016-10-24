/* A Bison parser, made by GNU Bison 3.0.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2013 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY__BIGQUERY_BUILD64_DEBUG_BIGQUERY_PARSER_BIG_QUERY_YY_HH_INCLUDED
# define YY__BIGQUERY_BUILD64_DEBUG_BIGQUERY_PARSER_BIG_QUERY_YY_HH_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int  bigquery_debug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    SHOW = 258,
    HISTORY = 259,
    HELP = 260,
    QUIT = 261,
    DEFINE = 262,
    CREATE = 263,
    DROP = 264,
    SELECT = 265,
    WITHIN = 266,
    RECORD = 267,
    AS = 268,
    FROM = 269,
    WHERE = 270,
    JOIN = 271,
    INNER = 272,
    LEFT = 273,
    OUTER = 274,
    ON = 275,
    GROUPBY = 276,
    HAVING = 277,
    ORDERBY = 278,
    DISTINCT = 279,
    DESC = 280,
    ASC = 281,
    LIMIT = 282,
    LOGICAL_OR = 283,
    LOGICAL_AND = 284,
    LOGICAL_NOT = 285,
    BITWISE_AND = 286,
    BITWISE_NOT = 287,
    BITWISE_OR = 288,
    BITWISE_XOR = 289,
    BITWISE_LEFT_SHIFT = 290,
    BITWISE_RIGHT_SHIFT = 291,
    EQUAL = 292,
    NOT_EQUAL = 293,
    LESS_THAN_OR_EQUAL = 294,
    LESS_THAN = 295,
    GREATER_THAN_OR_EQUAL = 296,
    GREATER_THAN = 297,
    SLASH = 298,
    DIV = 299,
    STAR = 300,
    PLUS = 301,
    MINUS = 302,
    REMAINDER = 303,
    CONTAINS = 304,
    DOT = 305,
    COLON = 306,
    COMMA = 307,
    SEMICOLON = 308,
    LPAREN = 309,
    RPAREN = 310,
    LSQUARE = 311,
    RSQUARE = 312,
    LCURLY = 313,
    RCURLY = 314,
    ENDOF = 315,
    LEX_ERROR = 316,
    WS = 317,
    INT = 318,
    FLOAT = 319,
    BOOLEAN = 320,
    ID = 321,
    STRING = 322
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE YYSTYPE;
union YYSTYPE
{
#line 34 "bigquery/parser/big_query.yy" /* yacc.c:1909  */

    int64_t _int;
    bool _bool;
    double _float;
    bigquery::compiler::StringMessage* _string;

    bigquery::compiler::ColumnPath* _column_path;
    bigquery::compiler::ColumnPathList* _column_path_list;
    bigquery::compiler::CreateTableStmt* _create_stmt;

    bigquery::compiler::DropTableStmt* _drop_stmt;
    bigquery::compiler::HelpStmt* _help_stmt;
    bigquery::compiler::ShowStmt* _show_stmt;
    bigquery::compiler::HistoryStmt* _history_stmt;
    bigquery::compiler::QuitStmt* _quit_stmt;
    bigquery::compiler::DefineTableStmt* _define_stmt;

    bigquery::compiler::JoinOperator _join_op;
    bigquery::compiler::Limit* _limit;
    bigquery::compiler::Operators  _operator;
    bigquery::compiler::OrderColumnPathList* _order_column_path_list;
    bigquery::compiler::OrderColumnPath* _order_column_path;
    bigquery::compiler::OrderType _order_type;

    bigquery::compiler::RawArguments* _raw_arguments;
    bigquery::compiler::RawAtomicExpression* _raw_atomic_expression;
    bigquery::compiler::RawExpressionList* _raw_expression_list;
    bigquery::compiler::RawExpression* _raw_expression;
    bigquery::compiler::RawFunction* _raw_function;
    bigquery::compiler::RawJoin* _raw_join;
    bigquery::compiler::RawTableList* _raw_table_list;
    bigquery::compiler::RawTable* _raw_table;
    bigquery::compiler::RawTargetList* _raw_target_list;
    bigquery::compiler::RawTarget* _raw_target;
    bigquery::compiler::RawWithin* _raw_within;

    bigquery::compiler::QueryStmt* _query_stmt;
    bigquery::compiler::SelectStmt* _select_stmt;

#line 162 "build64_debug/bigquery/parser/big_query.yy.hh" /* yacc.c:1909  */
};
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


extern YYSTYPE  bigquery_lval;
extern YYLTYPE  bigquery_lloc;
int  bigquery_parse (yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output);

#endif /* !YY__BIGQUERY_BUILD64_DEBUG_BIGQUERY_PARSER_BIG_QUERY_YY_HH_INCLUDED  */
