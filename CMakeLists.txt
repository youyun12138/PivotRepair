cmake_minimum_required(VERSION 3.20)

project(pivotrepaire)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)


aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/config src_dir_config)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/data/access src_dir_data_access)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/data/file src_dir_data_file)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/repair src_dir_repair)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/repair/procs src_dir_procs)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/task/algorithm src_dir_alg)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/task/algorithm/old_alg src_dir_alg_old)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/task src_dir_task)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/util src_dir_util)

set(source ${src_dir_util} ${src_dir_config} ${src_dir_data_file} ${src_dir_data_access} ${src_dir_procs} ${src_dir_alg_old} ${src_dir_alg} ${src_dir_task} ${src_dir_repair}  )

add_executable(master_main ${CMAKE_CURRENT_SOURCE_DIR}/src/master_main.cc)
target_sources(master_main PRIVATE ${source})
# Specify the C++ standard required for the target
target_compile_features(master_main PRIVATE cxx_std_14)
# Link the target with the libraries and their directories
target_link_libraries(master_main PUBLIC -lpthread -lsockpp -lisal)
# Add the include directories for the target
target_include_directories(master_main PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)


add_executable(node_main ${CMAKE_CURRENT_SOURCE_DIR}/src/node_main.cc)
target_sources(node_main PRIVATE ${source})
# Specify the C++ standard required for the target
target_compile_features(node_main PRIVATE cxx_std_14)
# Link the target with the libraries and their directories
target_link_libraries(node_main PUBLIC -lpthread -lsockpp -lisal)
# Add the include directories for the target
target_include_directories(node_main PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
