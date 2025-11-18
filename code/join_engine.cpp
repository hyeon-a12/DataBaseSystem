// code/join_engine.cpp
#include "file_io.h"
#include <vector>
#include <fstream>
#include <iostream>

using namespace std;

// CUSTOMER 블록 하나와 ORDERS 블록 하나를 받아서 조인 결과를 out에 씀
void joinCustomerOrdersBlock(
    const vector<Customer>& custBlock,
    const vector<Order>&    orderBlock,
    ofstream& out
) {
    for (const auto& c : custBlock) {
        for (const auto& o : orderBlock) {
            if (c.custkey == o.custkey) {
                // 결과 포맷은 팀에서 합의
                out << c.custkey << "|"
                    << c.name     << "|"
                    << o.orderkey << "|"
                    << o.orderdate << "|"
                    << o.totalprice << "|"
                    << c.mktsegment << "|"
                    << c.comment    << "|"
                    << o.comment
                    << "\n";
            }
        }
    }
}