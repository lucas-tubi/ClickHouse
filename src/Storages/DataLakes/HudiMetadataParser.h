#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/DataLakes/Metadata.h>

namespace DB
{

template <typename Configuration, typename MetadataReadHelper>
struct HudiMetadataParser
{
public:
    HudiMetadataParser<Configuration, MetadataReadHelper>();

    Metadata getMetadata(const Configuration & configuration, ContextPtr context);

private:
    struct Impl;
    std::shared_ptr<Impl> impl;
};

}
