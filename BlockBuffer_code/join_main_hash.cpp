// BlockBuffer_code/join_main_hash.cpp
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <chrono>

#include "file_io.h"   // Customer, Order, readCustomerBlock(), readOrderBlock()

using namespace std;

// 결과 한 줄 출력: C_CUSTKEY | C_NAME | O_ORDERKEY | O_ORDERDATE | O_TOTALPRICE
static inline void writeJoinResult(ofstream& fout,
                                   const Customer& c,
                                   const Order&    o)
{
    // 문자열 연결 대신, 연속된 << 연산으로 바로 출력
    fout << c.custkey   << '|'
         << c.name      << '|'
         << o.orderkey  << '|'
         << o.orderdate << '|'
         << o.totalprice << '\n';
}

int main(int argc, char* argv[])
{
    // C / C++ stdio 동기화 끄기 (입출력 약간 빨라짐)
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    // 1) 버퍼 크기 설정 --------------------------------------
    int bufferSize = DEFAULT_BUFFER_SIZE;   // file_io.h 에서 정의한 기본값 (예: 100)
    if (argc >= 2) {
        bufferSize = stoi(argv[1]);         // 예: ./run_join_hash 500
    }

    cout << "[INFO] Buffer size = " << bufferSize << " records\n";

    // 2) 입력 / 출력 파일 열기 --------------------------------
    ifstream c_fin("data/customer.tbl", ios::in | ios::binary);
    ifstream o_fin("data/orders.tbl",   ios::in | ios::binary);
    ofstream out("data/join_customer_orders_hash.tbl",
                 ios::out | ios::binary);

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

    // 출력 스트림에 조금 더 큰 버퍼 부여 (디스크 I/O 줄이기 위해)
    static char outBuf[1 << 20]; // 1MB
    out.rdbuf()->pubsetbuf(outBuf, sizeof(outBuf));

    auto startTime = chrono::high_resolution_clock::now();

    // 3) ORDERS 를 해시 테이블에 한 번만 적재 -----------------
    //    key = O_CUSTKEY, value = 그 고객의 모든 주문 벡터
    unordered_map<int, vector<Order>> ordersByCust;
    ordersByCust.reserve(1'600'000); // 대략 orders 개수보다 약간 크게

    vector<Order>    o_block;
    vector<Customer> c_block;
    o_block.reserve(bufferSize);
    c_block.reserve(bufferSize);

    long long orderCount = 0;
    long long custCount  = 0;
    long long joinCount  = 0;

    // 3-1) ORDERS 전체를 블록 단위로 읽어 해시 테이블에 저장
    while (readOrderBlock(o_fin, o_block, bufferSize)) {
        // readOrderBlock 안에서 o_block.clear() 를 해주고 있다고 가정
        for (const auto& o : o_block) {
            ordersByCust[o.custkey].push_back(o);
            ++orderCount;
        }
    }


    // 4) CUSTOMER 를 읽으면서 해시 테이블 조회 ----------------
    c_fin.clear();
    c_fin.seekg(0);   // 혹시 모를 EOF 플래그 제거 후 처음으로 이동

    int cBlockIdx = 0;

    while (readCustomerBlock(c_fin, c_block, bufferSize)) {
        ++cBlockIdx;
        // 디버깅용 로그는 필요하면만 찍기 (속도에 영향 커서 주석 처리해도 됨)
        // cout << "[INFO] CUSTOMER 블록 " << cBlockIdx
        //      << " (size=" << c_block.size() << ") 처리 중...\n";

        for (const auto& c : c_block) {
            ++custCount;

            auto it = ordersByCust.find(c.custkey);
            if (it == ordersByCust.end()) continue;   // 주문이 없는 고객

            const auto& ordersForCust = it->second;
            // 해당 고객의 모든 주문과 조인
            for (const auto& o : ordersForCust) {
                writeJoinResult(out, c, o);
                ++joinCount;
            }
        }
    }

    auto endTime = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = endTime - startTime;

    cout << "\n=== Hash Join 완료 ===\n";
    cout << "CUSTOMER 개수      : " << custCount  << "\n";
    cout << "ORDERS 개수        : " << orderCount << "\n";
    cout << "조인 결과 튜플 수  : " << joinCount  << "\n";
    cout << "총 실행 시간 (초)  : " << elapsed.count() << " s\n";
    cout << "결과 파일          : data/join_customer_orders_hash.tbl\n";

    return 0;
}

#ifdef _WIN32
// MinGW 에서 발생하는 WinMain 링크 에러 방지용 래퍼
int __stdcall WinMain(void*, void*, char*, int)
{
    return main(__argc, __argv);
}
#endif
