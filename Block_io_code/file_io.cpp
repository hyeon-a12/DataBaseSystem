// file_io.cpp
#include "file_io.h"

#include <sstream>
#include <iostream>

using namespace std;

const int DEFAULT_BUFFER_SIZE = 100;   // 기본 버퍼(블록) 크기

// -----------------------------
//  내부 유틸 함수 (헤더에 노출 X)
// -----------------------------

// delim(기본 '|')까지 읽어서 string으로 반환
static bool getField(istringstream& ss, string& out, char delim = '|')
{
    if (!getline(ss, out, delim)) return false;
    return true;
}

// '|'로 끝나는 정수 필드
static bool parseIntField(istringstream& ss, int& out)
{
    string tmp;
    if (!getField(ss, tmp, '|')) return false;
    out = stoi(tmp);
    return true;
}

// '|'로 끝나는 double 필드
static bool parseDoubleField(istringstream& ss, double& out)
{
    string tmp;
    if (!getField(ss, tmp, '|')) return false;
    out = stod(tmp);
    return true;
}

// 마지막 필드: 줄 끝까지 읽음. 마지막에 '|' 있으면 제거
static bool parseLastField(istringstream& ss, string& out)
{
    if (!getline(ss, out)) return false;
    if (!out.empty() && out.back() == '|')
        out.pop_back();
    return true;
}

// -----------------------------
//  레코드(한 줄) 읽기 구현부
// -----------------------------

bool readCustomerRecord(ifstream& fin, Customer& rec)
{
    string line;
    if (!getline(fin, line)) return false;
    if (line.empty()) return false;

    istringstream ss(line);

    // C_CUSTKEY | C_NAME | C_ADDRESS | C_NATIONKEY | C_PHONE |
    // C_ACCTBAL | C_MKTSEGMENT | C_COMMENT
    string dummy;

    if (!parseIntField   (ss, rec.custkey))    return false;
    if (!getField        (ss, rec.name))       return false;
    if (!getField        (ss, dummy))          return false; // ADDRESS (사용 X)
    if (!parseIntField   (ss, rec.nationkey))  return false;
    if (!getField        (ss, dummy))          return false; // PHONE (사용 X)
    if (!parseDoubleField(ss, rec.acctbal))    return false;
    if (!getField        (ss, rec.mktsegment)) return false;
    if (!parseLastField  (ss, rec.comment))    return false;

    return true;
}

bool readOrderRecord(ifstream& fin, Order& rec)
{
    string line;
    if (!getline(fin, line)) return false;
    if (line.empty()) return false;

    istringstream ss(line);

    // O_ORDERKEY | O_CUSTKEY | O_ORDERSTATUS | O_TOTALPRICE |
    // O_ORDERDATE | O_ORDERPRIORITY | O_CLERK |
    // O_SHIPPRIORITY | O_COMMENT
    string dummy;

    if (!parseIntField   (ss, rec.orderkey))   return false;
    if (!parseIntField   (ss, rec.custkey))    return false;
    if (!getField        (ss, dummy))          return false; // ORDERSTATUS
    if (!parseDoubleField(ss, rec.totalprice)) return false;
    if (!getField        (ss, rec.orderdate))  return false;
    if (!getField        (ss, rec.priority))   return false;
    if (!getField        (ss, dummy))          return false; // CLERK
    if (!getField        (ss, dummy))          return false; // SHIPPRIORITY
    if (!parseLastField  (ss, rec.comment))    return false;

    return true;
}

bool readLineitemRecord(ifstream& fin, Lineitem& rec)
{
    string line;
    if (!getline(fin, line)) return false;
    if (line.empty()) return false;

    istringstream ss(line);

    // L_ORDERKEY | L_PARTKEY | L_SUPPKEY | L_LINENUMBER |
    // L_QUANTITY | L_EXTENDEDPRICE | L_DISCOUNT | L_TAX |
    // L_RETURNFLAG | L_LINESTATUS | L_SHIPDATE | L_COMMITDATE |
    // L_RECEIPTDATE | L_SHIPINSTRUCT | L_SHIPMODE | L_COMMENT

    string tmp;

    if (!parseIntField   (ss, rec.orderkey))      return false;
    if (!parseIntField   (ss, rec.partkey))       return false;
    if (!parseIntField   (ss, rec.suppkey))       return false;
    if (!parseIntField   (ss, rec.linenumber))    return false;
    if (!parseDoubleField(ss, rec.quantity))      return false;
    if (!parseDoubleField(ss, rec.extendedprice)) return false;
    if (!parseDoubleField(ss, rec.discount))      return false;
    if (!parseDoubleField(ss, rec.tax))           return false;

    if (!getField        (ss, tmp))               return false;
    rec.returnflag = tmp.empty() ? '\0' : tmp[0];

    if (!getField        (ss, tmp))               return false;
    rec.linestatus = tmp.empty() ? '\0' : tmp[0];

    if (!getField        (ss, rec.shipdate))      return false;
    if (!getField        (ss, rec.commitdate))    return false;
    if (!getField        (ss, rec.receiptdate))   return false;
    if (!getField        (ss, rec.shipinstruct))  return false;
    if (!getField        (ss, rec.shipmode))      return false;
    if (!parseLastField  (ss, rec.comment))       return false;

    return true;
}

// -----------------------------
//  공통 블록 읽기 템플릿
// -----------------------------

template <typename RecordType>
static bool readBlockImpl(ifstream& fin,
                          bool (*readRecordFunc)(ifstream&, RecordType&),
                          vector<RecordType>& block,
                          int bufferSize)
{
    block.clear();
    block.reserve(bufferSize);

    RecordType rec;
    for (int i = 0; i < bufferSize; ++i) {
        streampos pos = fin.tellg();
        if (!readRecordFunc(fin, rec)) {
            if (fin.eof()) break;   // 정상 EOF
            // 에러나 이상한 줄이면 복구 후 중단
            fin.clear();
            fin.seekg(pos);
            break;
        }
        block.push_back(rec);
    }

    return !block.empty();  // 하나라도 읽었으면 true
}

// -----------------------------
//  테이블별 블록 읽기 래퍼
// -----------------------------

bool readCustomerBlock(ifstream& fin,
                       vector<Customer>& block,
                       int bufferSize)
{
    return readBlockImpl<Customer>(fin, readCustomerRecord, block, bufferSize);
}

bool readOrderBlock(ifstream& fin,
                    vector<Order>& block,
                    int bufferSize)
{
    return readBlockImpl<Order>(fin, readOrderRecord, block, bufferSize);
}

bool readLineitemBlock(ifstream& fin,
                       vector<Lineitem>& block,
                       int bufferSize)
{
    return readBlockImpl<Lineitem>(fin, readLineitemRecord, block, bufferSize);
}