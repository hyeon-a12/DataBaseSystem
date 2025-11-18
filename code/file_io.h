// file_io.h
#pragma once

#include <string>
#include <vector>
#include <fstream>

// -----------------------------
//  공통 설정
// -----------------------------
extern const int DEFAULT_BUFFER_SIZE;   // file_io.cpp에서 정의

// -----------------------------
//  TPC-H 테이블 구조체 정의
//  (조인/결과에 필요한 필드만 사용)
// -----------------------------

// CUSTOMER: C_CUSTKEY | C_NAME | C_ADDRESS | C_NATIONKEY | C_PHONE |
//           C_ACCTBAL | C_MKTSEGMENT | C_COMMENT
struct Customer {
    int    custkey;       // C_CUSTKEY
    std::string name;     // C_NAME
    int    nationkey;     // C_NATIONKEY
    double acctbal;       // C_ACCTBAL
    std::string mktsegment; // C_MKTSEGMENT
    std::string comment;  // C_COMMENT
};

// ORDERS: O_ORDERKEY | O_CUSTKEY | O_ORDERSTATUS | O_TOTALPRICE |
//         O_ORDERDATE | O_ORDERPRIORITY | O_CLERK | O_SHIPPRIORITY | O_COMMENT
struct Order {
    int    orderkey;      // O_ORDERKEY
    int    custkey;       // O_CUSTKEY
    std::string orderdate; // O_ORDERDATE
    double totalprice;    // O_TOTALPRICE
    std::string priority; // O_ORDERPRIORITY
    std::string comment;  // O_COMMENT
};

// LINEITEM: L_ORDERKEY | L_PARTKEY | L_SUPPKEY | L_LINENUMBER |
//           L_QUANTITY | L_EXTENDEDPRICE | L_DISCOUNT | L_TAX |
//           L_RETURNFLAG | L_LINESTATUS | L_SHIPDATE | L_COMMITDATE |
//           L_RECEIPTDATE | L_SHIPINSTRUCT | L_SHIPMODE | L_COMMENT
struct Lineitem {
    int    orderkey;       // L_ORDERKEY
    int    partkey;        // L_PARTKEY
    int    suppkey;        // L_SUPPKEY
    int    linenumber;     // L_LINENUMBER
    double quantity;       // L_QUANTITY
    double extendedprice;  // L_EXTENDEDPRICE
    double discount;       // L_DISCOUNT
    double tax;            // L_TAX
    char   returnflag;     // L_RETURNFLAG
    char   linestatus;     // L_LINESTATUS
    std::string shipdate;      // L_SHIPDATE
    std::string commitdate;    // L_COMMITDATE
    std::string receiptdate;   // L_RECEIPTDATE
    std::string shipinstruct;  // L_SHIPINSTRUCT
    std::string shipmode;      // L_SHIPMODE
    std::string comment;       // L_COMMENT
};

// -----------------------------
//  레코드(한 줄) 읽기 함수
// -----------------------------

bool readCustomerRecord(std::ifstream& fin, Customer& rec);
bool readOrderRecord   (std::ifstream& fin, Order& rec);
bool readLineitemRecord(std::ifstream& fin, Lineitem& rec);

// -----------------------------
//  버퍼(블록) 단위 읽기 함수
//  - 최대 bufferSize개의 레코드를 읽어서 vector에 채움
//  - 실제 읽은 개수는 block.size()로 알 수 있음
//  - 읽은 레코드가 하나도 없으면 false 반환 (EOF)
// -----------------------------

bool readCustomerBlock(std::ifstream& fin,
                       std::vector<Customer>& block,
                       int bufferSize = DEFAULT_BUFFER_SIZE);

bool readOrderBlock(std::ifstream& fin,
                    std::vector<Order>& block,
                    int bufferSize = DEFAULT_BUFFER_SIZE);

bool readLineitemBlock(std::ifstream& fin,
                       std::vector<Lineitem>& block,
                       int bufferSize = DEFAULT_BUFFER_SIZE);
