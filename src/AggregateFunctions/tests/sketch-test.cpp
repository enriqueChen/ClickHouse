#include <iostream>
#include <tuple_sketch.hpp>

int main() {
    auto a = datasketches::update_tuple_sketch<float>::builder().build();
    a.update(1,1);
    return 0;
}
