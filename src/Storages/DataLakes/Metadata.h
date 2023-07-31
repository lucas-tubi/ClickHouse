#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

struct Metadata
{

    using KeyWithPartitionValues = std::map<String, std::optional<std::map<String, String>>>;

    Metadata(Strings keys_)
    {
        for(const auto & k: keys_) {
            key_with_partition_values.emplace(k, std::nullopt);
        }
        partitions = NamesAndTypesList();
    }

    Metadata(KeyWithPartitionValues _keys, NamesAndTypesList _partitions): key_with_partition_values(_keys), partitions(_partitions)
    {

    }



    KeyWithPartitionValues key_with_partition_values;
    NamesAndTypesList partitions;
};

}
