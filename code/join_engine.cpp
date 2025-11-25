// code/join_engine.cpp
#include "file_io.h"
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>   
#include <string>  
#include <omp.h>
using namespace std;

// CUSTOMER 블록 하나와 ORDERS 블록 하나를 받아서 조인 결과를 out에 씀
// void joinCustomerOrdersBlock(
//     const vector<Customer>& custBlock,
//     const vector<Order>&    orderBlock,
//     ofstream& out
// ) {
//     for (const auto& c : custBlock) {
//         for (const auto& o : orderBlock) {
//             if (c.custkey == o.custkey) {
//                 // 결과 포맷은 팀에서 합의
//                 out << c.custkey << "|"
//                     << c.name     << "|"
//                     << o.orderkey << "|"
//                     << o.orderdate << "|"
//                     << o.totalprice << "|"
//                     << c.mktsegment << "|"
//                     << c.comment    << "|"
//                     << o.comment
//                     << "\n";
//             }
//         }
//     }
// }

//2번째 방법
// void joinCustomerOrdersBlock(const vector<Customer>& custBlock,
//                              const vector<Order>& orderBlock,
//                              ofstream& out) {
//     std::ostringstream buf;

//     for (const auto& c : custBlock) {
//         for (const auto& o : orderBlock) {
//             if (c.custkey == o.custkey) {
//                 buf << c.custkey << '|' << ... << '\n';
//             }
//         }
//     }

//     out << buf.str();  // 블록 단위로 한 번에 기록
// }


//3번째 방법

// #include <sstream>  // 꼭 필요!
// #include <string>   // 꼭 필요!

// void joinCustomerOrdersBlock(const vector<Customer>& custBlock,
//                              const vector<Order>&    orderBlock,
//                              ofstream& out)
// {
//     std::ostringstream buf;

//     for (const auto& c : custBlock) {
//         for (const auto& o : orderBlock) {

//             if (c.custkey == o.custkey) {

//                 buf << c.custkey               << '|'
//                     << std::string(c.name)     << '|'
//                     << o.orderkey              << '|'
//                     << std::string(o.orderdate)<< '|'
//                     << o.totalprice            << '|'
//                     << std::string(c.mktsegment)<< '|'
//                     << std::string(c.comment)   << '|'
//                     << std::string(o.comment)   << '\n';
//             }
//         }
//     }

//     out << buf.str();     // 버퍼를 한 번에 출력
// }

//4번째 방법
#include "file_io.h"
#include <vector>
#include <fstream>
#include <iostream>
#include <unordered_map>  // ★ 추가
using namespace std;

// CUSTOMER 블록 하나와 ORDERS 블록 하나를 받아서 조인 결과를 out에 씀
void joinCustomerOrdersBlock(const vector<Customer>& custBlock,
                             const vector<Order>&    orderBlock,
                             ofstream& out)
{
    // 1. ORDERS 블록으로 해시 인덱스 생성 (custkey -> Order*)
    unordered_multimap<int, const Order*> orderIndex;
    orderIndex.reserve(orderBlock.size() * 2);  // 리해시 줄이기 위한 여유

    for (const auto& o : orderBlock) {
        orderIndex.emplace(o.custkey, &o);
    }

    // 2. CUSTOMER 블록의 각 레코드에 대해,
    //    같은 custkey를 가진 주문들만 찾아서 조인
    for (const auto& c : custBlock) {

        auto range = orderIndex.equal_range(c.custkey);
        for (auto it = range.first; it != range.second; ++it) {
            const Order* o = it->second;

            // 결과 포맷은 기존 코드와 동일
            out << c.custkey      << '|'
                << c.name         << '|'
                << o->orderkey    << '|'
                << o->orderdate   << '|'
                << o->totalprice  << '|'
                << c.mktsegment   << '|'
                << c.comment      << '|'
                << o->comment     << '\n';
        }
    }
}



