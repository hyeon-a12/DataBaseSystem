
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>      // 실행 시간 측정용
#include "file_io.h"


using namespace std;

// 출력 포맷: C_CUSTKEY | C_NAME | O_ORDERKEY | O_ORDERDATE | O_TOTALPRICE
static void writeJoinResult(ofstream& fout,
                            const Customer& c,
                            const Order& o)
{
    fout << c.custkey << "|"
         << c.name << "|"
         << o.orderkey << "|"
         << o.orderdate << "|"
         << o.totalprice << "\n";
}

int main(int argc, char* argv[])
{
    // 1) 버퍼 크기 설정 ---------------------------------------
    int bufferSize = DEFAULT_BUFFER_SIZE;   // 기본값 100 (file_io.cpp에 정의)

    if (argc >= 2) {
        bufferSize = stoi(argv[1]);         // ./run_join 500 이런 식으로 변경
    }

    cout << "[INFO] Buffer size = " << bufferSize << " records\n";

    // 2) 입력 / 출력 파일 열기 ---------------------------------
    ifstream c_fin("data/customer.tbl");
    ifstream o_fin("data/orders.tbl");
    ofstream out("data/join_customer_orders.tbl");  // 결과 파일

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

    // 3) 블록 버퍼 선언 ---------------------------------------
    vector<Customer> c_block;
    vector<Order>    o_block;

    long long joinCount = 0;   // 조인된 튜플 개수 카운트

    auto startTime = chrono::high_resolution_clock::now();

    // 4) 외부 루프: CUSTOMER를 블록 단위로 읽기 ---------------
    int outerBlockIdx = 0;

    while (readCustomerBlock(c_fin, c_block, bufferSize)) {
        ++outerBlockIdx;
        cout << "[INFO] CUSTOMER 블록 " << outerBlockIdx
             << " (size=" << c_block.size() << ") 처리 중...\n";

        // 🔁 내부 루프를 위해 ORDERS 파일 처음으로 되돌리기
        o_fin.clear();        // EOF/에러 플래그 초기화
        o_fin.seekg(0);       // 파일 포인터 맨 앞으로

        // 5) 내부 루프: ORDERS를 블록 단위로 읽기 --------------
        while (readOrderBlock(o_fin, o_block, bufferSize)) {

            // 6) Block Nested Loops Join 핵심 부분 -------------
            for (const auto& c : c_block) {
                for (const auto& o : o_block) {
                    if (c.custkey == o.custkey) {   // 조인 조건
                        writeJoinResult(out, c, o);
                        ++joinCount;
                    }
                }
            }
        }
    }

    auto endTime = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = endTime - startTime;

    cout << "\n=== Join 완료 ===\n";
    cout << "조인 결과 튜플 수   : " << joinCount << "\n";
    cout << "총 실행 시간 (초)   : " << elapsed.count() << " s\n";
    cout << "결과 파일           : data/join_customer_orders.tbl\n";

    return 0;
}
