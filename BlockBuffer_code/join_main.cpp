#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>

#include "../code/file_io.h"
#include "../code/join_engine.cpp"

using namespace std;

int main(int argc, char* argv[]) {

    int bufferSize = DEFAULT_BUFFER_SIZE;
    if (argc >= 2) bufferSize = stoi(argv[1]);

    cout << "[INFO] bufferSize = " << bufferSize << " 레코드" << endl;

    ifstream custFile("data/customer.tbl");
    ifstream orderFile("data/orders.tbl");
    ofstream out("result/result_cust_orders.tbl");

    if (!custFile.is_open() || !orderFile.is_open() || !out.is_open()) {
        cerr << "[ERROR] 파일을 열 수 없습니다!" << endl;
        return 1;
    }

    auto start = chrono::high_resolution_clock::now();

    //기존
    // vector<Customer> custBlock;
    // vector<Order> orderBlock;

    vector<Customer> custBlock;
    vector<Order>   orderBlock;

    custBlock.reserve(bufferSize);
    orderBlock.reserve(bufferSize);

    int custBlockIdx = 0;

    // CUSTOMER 블록 반복
    while (true) {
        bool custOk = readCustomerBlock(custFile, custBlock, bufferSize);
        if (!custOk) break;

        cout << "[INFO] CUSTOMER 블록 " << custBlockIdx++
             << " (size=" << custBlock.size() << ") 처리 중..." << endl;

        // ORDERS 파일은 매 CUSTOMER 블록마다 처음부터 읽기
        orderFile.clear();
        orderFile.seekg(0, ios::beg);

        // ORDER 반복
        while (true) {
            bool orderOk = readOrderBlock(orderFile, orderBlock, bufferSize);
            if (!orderOk) break;

            // 블록 vs 블록 조인
            joinCustomerOrdersBlock(custBlock, orderBlock, out);
        }
    }

auto end = chrono::high_resolution_clock::now();
auto ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();

// 대략적인 메모리 Footprint 계산
size_t custRecordSize  = sizeof(Customer);
size_t orderRecordSize = sizeof(Order);

// CUSTOMER 블록 1개 + ORDER 블록 1개를 메모리에 올린다고 가정
size_t memBytes = (custRecordSize + orderRecordSize) * bufferSize;

cout << "[RESULT] bufferSize=" << bufferSize 
     << " 실행 시간: " << ms << " ms"
     << " , 메모리 Footprint ≈ " << memBytes / (1024.0 * 1024.0) << " MB"
     << endl;

return 0;
}

