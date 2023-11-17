// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "in_mem_graph_store.h"
#include "utils.h"
#include "variablebyte.h"

namespace diskann
{
    
InMemGraphStore::InMemGraphStore(const size_t total_pts, const size_t reserve_graph_degree)
    : AbstractGraphStore(total_pts, reserve_graph_degree)
{
    this->resize_graph(total_pts);
    for (size_t i = 0; i < total_pts; i++)
    {
        _graph2[i].reserve(2 * reserve_graph_degree * sizeof(uint32_t));
        _degree_counts[i] = 0;
    }
}

std::tuple<uint32_t, uint32_t, size_t> InMemGraphStore::load(const std::string &index_path_prefix,
                                                             const size_t num_points)
{
    return load_impl(index_path_prefix, num_points);
}
int InMemGraphStore::store(const std::string &index_path_prefix, const size_t num_points,
                           const size_t num_frozen_points, const uint32_t start)
{
    return save_graph(index_path_prefix, num_points, num_frozen_points, start);
}
std::vector<location_t> InMemGraphStore::get_neighbours(const location_t i) const
{
    auto* src = const_cast<uint8_t*>(_graph2[i].data());
    std::vector<location_t> neighbours;
    if(_degree_counts[i] != 0){
        neighbours.resize(_degree_counts[i]);
        auto nvalue = neighbours.size();
        VariableByte().decodeFromByteArray(src, _graph2[i].size(), neighbours.data(), nvalue);
    }
    if(i == 22704){
        diskann::cout << "\n====== GET NEIGHBOURS =====\n"  << std::flush;
        for (auto n : neighbours)
            diskann::cout <<  std::to_string(n) << "\n" << std::flush;
        diskann::cout << "====== GET NEIGHBOURS =====\n "  << std::flush;

    }
    

    // std::vector<uint8_t> compressed_data = _graph2[i];
    // // diskann::cout << i << " " << _graph2[i].size() << "\n" << std::flush;
    // // // Decode the compressed data to get the original neighbours
    // std::vector<location_t> neighbours = decode_data(compressed_data, i);

    return neighbours;
    // return _graph.at(i);
}

void InMemGraphStore::add_neighbour(const location_t i, location_t neighbour_id)
{

    if(i == 22704){
        diskann::cout << "\n====== ADD NEIGHBOURS =====\n "  << std::flush;
        diskann::cout << i << " " << neighbour_id << "\n" << std::flush;
        diskann::cout << "\n====== ADD NEIGHBOURS =====\n "  << std::flush;
    }


    std::vector<location_t> neighbours;
    if (neighbours.size() > 0) {
        auto* src = const_cast<uint8_t*>(_graph2[i].data());
        
        neighbours.resize(_degree_counts[i]);
        auto nvalue = neighbours.size();
        VariableByte().decodeFromByteArray(src, _graph2[i].size(), neighbours.data(), nvalue);
    }
    neighbours.push_back(neighbour_id);
    _degree_counts[i] += 1;

    {
        auto* src = const_cast<uint32_t*>(neighbours.data());
        std::vector<std::uint8_t> buf;
        buf.resize(neighbours.size() * 8);
        auto nvalue = buf.size();
        VariableByte().encodeToByteArray(src, neighbours.size(), buf.data(), nvalue);
    
        _graph2[i].assign(buf.begin(), buf.begin() + nvalue);    
    }
    // std::vector<uint32_t> data = {neighbour_id};
    // std::vector<uint8_t> compressed_data(2 * data.size() * sizeof(uint32_t));
    // size_t compressed_size = VarIntGB<>().encodeArray(data.data(), data.size(), compressed_data.data());
    // compressed_data.resize(compressed_size);
    // _graph2[i].insert(_graph2[i].end(), compressed_data.begin(), compressed_data.end());
    

    if (_max_observed_degree < _degree_counts[i])
    {
        _max_observed_degree = (uint32_t)(_degree_counts[i]);
    }
}

void InMemGraphStore::clear_neighbours(const location_t i)
{
    _graph2[i].clear();
    _degree_counts[i] = 0;
};
void InMemGraphStore::swap_neighbours(const location_t a, location_t b)
{
    _graph2[a].swap(_graph2[b]);
    auto tmp = _degree_counts[a];
    _degree_counts[a] = _degree_counts[b];
    _degree_counts[b] = tmp;

};


void InMemGraphStore::set_neighbours(const location_t i, std::vector<location_t> &neighbours)
{

    if(i == 22704){
        diskann::cout << "\n====== SET NEIGHBOURS =====\n "  << std::flush;
        for (auto n : neighbours)
            diskann::cout << n << "\n" << std::flush;
        diskann::cout << "====== SET NEIGHBOURS =====\n "  << std::flush;
    }

    auto* src = const_cast<uint32_t*>(neighbours.data());
    std::vector<std::uint8_t> buf;
    buf.resize(neighbours.size() * 8);
    auto nvalue = buf.size();
    VariableByte().encodeToByteArray(src, neighbours.size(), buf.data(), nvalue);
    _graph2[i].assign(buf.begin(), buf.begin() + nvalue);

    _degree_counts[i] = neighbours.size();

    if (_max_observed_degree < neighbours.size())
    {
        _max_observed_degree = (uint32_t)(neighbours.size());
    }
}

size_t InMemGraphStore::resize_graph(const size_t new_size)
{
    _graph2.resize(new_size);
    _degree_counts.resize(new_size);

    set_total_points(new_size);
    return _graph2.size();
}

void InMemGraphStore::clear_graph()
{
    _graph2.clear();
    _degree_counts.clear();
}

// #ifdef EXEC_ENV_OLS
// std::tuple<uint32_t, uint32_t, size_t> InMemGraphStore::load_impl(AlignedFileReader &reader, size_t expected_num_points)
// {
//     size_t expected_file_size;
//     size_t file_frozen_pts;
//     uint32_t start;

//     auto max_points = get_max_points();
//     int header_size = 2 * sizeof(size_t) + 2 * sizeof(uint32_t);
//     std::unique_ptr<char[]> header = std::make_unique<char[]>(header_size);
//     read_array(reader, header.get(), header_size);

//     expected_file_size = *((size_t *)header.get());
//     _max_observed_degree = *((uint32_t *)(header.get() + sizeof(size_t)));
//     start = *((uint32_t *)(header.get() + sizeof(size_t) + sizeof(uint32_t)));
//     file_frozen_pts = *((size_t *)(header.get() + sizeof(size_t) + sizeof(uint32_t) + sizeof(uint32_t)));

//     diskann::cout << "From graph header, expected_file_size: " << expected_file_size
//                   << ", _max_observed_degree: " << _max_observed_degree << ", _start: " << start
//                   << ", file_frozen_pts: " << file_frozen_pts << std::endl;

//     diskann::cout << "Loading vamana graph from reader..." << std::flush;

//     // If user provides more points than max_points
//     // resize the _graph to the larger size.
//     if (get_total_points() < expected_num_points)
//     {
//         diskann::cout << "resizing graph to " << expected_num_points << std::endl;
//         this->resize_graph(expected_num_points);
//     }

//     uint32_t nodes_read = 0;
//     size_t cc = 0;
//     size_t graph_offset = header_size;
//     while (nodes_read < expected_num_points)
//     {
//         uint32_t k;
//         read_value(reader, k, graph_offset);
//         graph_offset += sizeof(uint32_t);
//         std::vector<uint32_t> tmp(k);
//         tmp.reserve(k);
//         read_array(reader, tmp.data(), k, graph_offset);
//         graph_offset += k * sizeof(uint32_t);
//         cc += k;
//         _graph[nodes_read].swap(tmp);
//         nodes_read++;
//         if (nodes_read % 1000000 == 0)
//         {
//             diskann::cout << "." << std::flush;
//         }
//         if (k > _max_range_of_graph)
//         {
//             _max_range_of_graph = k;
//         }
//     }

//     diskann::cout << "done. Index has " << nodes_read << " nodes and " << cc << " out-edges, _start is set to " << start
//                   << std::endl;
//     return std::make_tuple(nodes_read, start, file_frozen_pts);
// }
// #endif

std::tuple<uint32_t, uint32_t, size_t> InMemGraphStore::load_impl(const std::string &filename,
                                                                  size_t expected_num_points)
{
    size_t expected_file_size;
    size_t file_frozen_pts;
    uint32_t start;
    size_t file_offset = 0; // will need this for single file format support

    std::ifstream in;
    in.exceptions(std::ios::badbit | std::ios::failbit);
    in.open(filename, std::ios::binary);
    in.seekg(file_offset, in.beg);
    in.read((char *)&expected_file_size, sizeof(size_t));
    in.read((char *)&_max_observed_degree, sizeof(uint32_t));
    in.read((char *)&start, sizeof(uint32_t));
    in.read((char *)&file_frozen_pts, sizeof(size_t));
    size_t vamana_metadata_size = sizeof(size_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(size_t);

    diskann::cout << "From graph header, expected_file_size: " << expected_file_size
                  << ", _max_observed_degree: " << _max_observed_degree << ", _start: " << start
                  << ", file_frozen_pts: " << file_frozen_pts << std::endl;

    diskann::cout << "Loading vamana graph " << filename << "..." << std::flush;

    // If user provides more points than max_points
    // resize the _graph to the larger size.
    if (get_total_points() < expected_num_points)
    {
        diskann::cout << "resizing graph to " << expected_num_points << std::endl;
        this->resize_graph(expected_num_points);
    }

    size_t bytes_read = vamana_metadata_size;
    size_t cc = 0;
    uint32_t nodes_read = 0;
    while (bytes_read != expected_file_size)
    {
        uint32_t k;
        in.read((char *)&k, sizeof(uint32_t));

        if (k == 0)
        {
            diskann::cerr << "ERROR: Point found with no out-neighbours, point#" << nodes_read << std::endl;
        }

        cc += k;
        ++nodes_read;
        std::vector<uint8_t> tmp(k);
        tmp.reserve(k);
        in.read((char *)tmp.data(), k * sizeof(uint8_t));
        _graph2[nodes_read - 1].assign(tmp.begin(), tmp.end());
        bytes_read += sizeof(uint8_t) * ((size_t)k + sizeof(uint32_t));
        if (nodes_read % 10000000 == 0)
            diskann::cout << "." << std::flush;
        if (k > _max_range_of_graph)
        {
            _max_range_of_graph = k;
        }
    }

    diskann::cout << "done. Index has " << nodes_read << " nodes and " << cc << " out-edges, _start is set to " << start
                  << std::endl;
    return std::make_tuple(nodes_read, start, file_frozen_pts);
}

int InMemGraphStore::save_graph(const std::string &index_path_prefix, const size_t num_points,
                                const size_t num_frozen_points, const uint32_t start)
{
    std::ofstream out;
    open_file_to_write(out, index_path_prefix);

    size_t file_offset = 0;
    out.seekp(file_offset, out.beg);
    size_t index_size = 24;
    uint32_t max_degree = 0;
    out.write((char *)&index_size, sizeof(uint64_t));
    out.write((char *)&_max_observed_degree, sizeof(uint32_t));
    uint32_t ep_u32 = start;
    out.write((char *)&ep_u32, sizeof(uint32_t));
    out.write((char *)&num_frozen_points, sizeof(size_t));

    // Note: num_points = _nd + _num_frozen_points
    for (uint32_t i = 0; i < num_points; i++)
    {
        uint32_t GK = (uint32_t)_graph2[i].size();
        out.write((char *)&GK, sizeof(uint32_t));
        out.write((char *)_graph2[i].data(), GK * sizeof(uint8_t));
        max_degree = _degree_counts[i] > max_degree ? (uint32_t)_degree_counts[i] : max_degree;
        index_size += (size_t)(sizeof(uint8_t) * GK + sizeof(uint32_t));
    }
    out.seekp(file_offset, out.beg);
    out.write((char *)&index_size, sizeof(uint64_t));
    out.write((char *)&max_degree, sizeof(uint32_t));
    out.close();
    return (int)index_size;
}

size_t InMemGraphStore::get_max_range_of_graph()
{
    return _max_range_of_graph;
}

uint32_t InMemGraphStore::get_max_observed_degree()
{
    return _max_observed_degree;
}

} // namespace diskann
