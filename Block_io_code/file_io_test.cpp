#include <iostream>
#include <fstream>
#include <vector>
#include "file_io.h"

using namespace std;

int main() {

    ifstream fin("data/orders.tbl");
    if (!fin.is_open()) {
        cout << "파일을 열 수 없습니다!" << endl;
        return 0;
    }

    vector<Order> block;

    cout << "=== 첫 번째 블록 읽기 테스트 ===" << endl;

    if (readOrderBlock(fin, block)) {
        cout << "블록 크기: " << block.size() << endl;

        // 블록에서 앞 3개만 출력하면서 데이터 정상 확인
        for (int i = 0; i < 3 && i < block.size(); i++) {
            cout << block[i].orderkey << " | "
                 << block[i].custkey << " | "
                 << block[i].orderdate << endl;
        }
    } else {
        cout << "데이터가 없음" << endl;
    }

    fin.close();
    return 0;
}