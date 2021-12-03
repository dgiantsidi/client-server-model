# Parent project does not export its library target, so this CML implicitly
# depends on being added from it, i.e. the testing is done only from the build
# tree and is not feasible from an install location

project(clt-svr-modelTests LANGUAGES CXX)

add_executable(clt-svr-model_test source/clt-svr-model_test.cpp)
target_link_libraries(clt-svr-model_test PRIVATE clt-svr-proto_lib ${Protobuf_LIBRARIES})
target_compile_features(clt-svr-model_test PRIVATE cxx_std_20)

add_test(NAME clt-svr-model_test COMMAND clt-svr-model_test)
add_test(NAME small-clt-svr_test COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/source/small-test.py ${CMAKE_BINARY_DIR} ${CMAKE_SOURCE_DIR})

add_folders(Test)
