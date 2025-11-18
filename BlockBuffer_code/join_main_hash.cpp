// BlockBuffer_code/join_main_hash.cpp
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <chrono>

#include "file_io.h"   // 기존에 만든 Customer, Order, readXXXBlock 선언

using namespace std;

// 출력 포맷: C_CUSTKEY | C_NAME | O_ORDERKEY | O_ORDERDATE | O_TOTALPRICE
static void writeJoinResult(ofstream& fout,
                            const Customer& c,
                            const Order& o)
{
    fout << c.custkey   << "|"
         << c.name      << "|"
         << o.orderkey  << "|"
         << o.orderdate << "|"
         << o.totalprice << "\n";
}

int main(int argc, char* argv[])
{
    // 1) 버퍼 크기 설정 (블록 IO 재사용용 – 해시 조인 자체는 O(N+M))
    int bufferSize = DEFAULT_BUFFER_SIZE;   // file_io.h 에 정의해 둔 값 (예: 100)
    if (argc >= 2) {
        bufferSize = stoi(argv[1]);         // ./run_join_hash 1000 이런 식으로 조절 가능
    }

    cout << "[INFO] Buffer size = " << bufferSize << " records\n";

    // 2) 입력 / 출력 파일 열기 -----------------------------------------
    ifstream c_fin("data/customer.tbl");
    ifstream o_fin("data/orders.tbl");
    ofstream out("data/join_customer_orders_hash.tbl");  // 결과 파일

    if (!c_fin.is_open()) {
        cerr << "❌ customer.tbl 파일을 열 수 없습니다!\n";
        return 1;
    }
    if (!o_fin.is_open()) {
        cerr << "❌ orders.tbl 파일을 열 수 없습니다!\n";
        return 1;
    }
    if (!out.is_open()) {
        cerr << "❌ 결과 파일을 만들 수 없습니다!\n";
        return 1;
    }

    auto startTime = chrono::high_resolution_clock::now();

    // 3) ORDERS를 해시 테이블에 한 번만 적재 ---------------------------
    //    key = O_CUSTKEY, value = 그 고객의 모든 주문 벡터
    unordered_map<int, vector<Order>> ordersByCust;
    ordersByCust.reserve(200000); // 대략적인 버킷 수(적당히 넉넉하게)

    vector<Order> o_block;
    long long orderCount = 0;

    while (readOrderBlock(o_fin, o_block, bufferSize)) {
        for (const auto& o : o_block) {
            ordersByCust[o.custkey].push_back(o);
            ++orderCount;
        }
    }

    cout << "[INFO] ORDERS " << orderCount
         << "건을 해시 테이블에 적재 완료 (bucket_count="
         << ordersByCust.bucket_count() << ")\n";

    // 4) CUSTOMER를 읽으면서 해시 테이블 조회 -------------------------
    c_fin.clear();
    c_fin.seekg(0);       // 혹시 몰라서 파일 포인터 처음으로

    vector<Customer> c_block;
    long long joinCount = 0;
    long long custCount = 0;
    int blockIdx = 0;

    while (readCustomerBlock(c_fin, c_block, bufferSize)) {
        ++blockIdx;
        cout << "[INFO] CUSTOMER 블록 " << blockIdx
             << " (size=" << c_block.size() << ") 처리 중...\n";

        for (const auto& c : c_block) {
            ++custCount;

            auto it = ordersByCust.find(c.custkey);
            if (it == ordersByCust.end()) continue;   // 주문 없는 고객

            // 해당 고객의 모든 주문과 조인
            const auto& ordersForCust = it->second;
            for (const auto& o : ordersForCust) {
                writeJoinResult(out, c, o);
                ++joinCount;
            }
        }
    }

    auto endTime = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = endTime - startTime;

    cout << "\n=== Hash Join 완료 ===\n";
    cout << "CUSTOMER 개수      : " << custCount << "\n";
    cout << "ORDERS 개수        : " << orderCount << "\n";
    cout << "조인 결과 튜플 수  : " << joinCount << "\n";
    cout << "총 실행 시간 (초)  : " << elapsed.count() << " s\n";
    cout << "결과 파일          : data/join_customer_orders_hash.tbl\n";

    return 0;
}

#ifdef _WIN32
// windows.h 없이 WinMain만 직접 선언
int __stdcall WinMain(void* hInst, void* hPrev, char* cmdLine, int showCmd)
{
    // MinGW에서 제공하는 전역 변수 __argc, __argv 사용
    return main(__argc, __argv);
}
#endif
