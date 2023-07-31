#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/DataLakes/Metadata.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>
#include <filesystem>


namespace DB
{

template <typename Storage, typename Name, typename MetadataParser>
class IStorageDataLake : public Storage
{
public:
    static constexpr auto name = Name::name;
    using Configuration = typename Storage::Configuration;

    template <class ...Args>
    explicit IStorageDataLake(const Configuration & configuration_, ContextPtr context_, Args && ...args)
        : Storage(getConfigurationForDataRead(configuration_, context_), context_, std::forward<Args>(args)...)
        , base_configuration(configuration_)
        , log(&Poco::Logger::get(getName())) {}

    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        Configuration & base_configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr local_context)
    {
        auto configuration = getConfigurationForDataRead(base_configuration, local_context);
        return Storage::getTableStructureFromData(configuration, format_settings, local_context);
    }

    static Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context)
    {
        return Storage::getConfiguration(engine_args, local_context, /* get_format_from_file */false);
    }

    Configuration updateConfigurationAndGetCopy(ASTPtr query, ContextPtr local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(query, local_context);
        return Storage::getConfiguration();
    }

    void updateConfiguration(ContextPtr local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(std::nullopt, local_context);
    }

private:
    static Configuration getConfigurationForDataRead(
        const Configuration & base_configuration, ContextPtr local_context, std::optional<ASTPtr> query = std::nullopt)
    {
        auto configuration{base_configuration};
        configuration.update(local_context);
        configuration.static_configuration = true;

        Metadata metadata = MetadataParser().getMetadata(configuration, local_context);

        /// Create a virtual block with one row to construct filter
        if (query && query)
        {
            Block virtual_header;
            const NamesAndTypesList partitions = metadata.partitions;
            for (const auto & p : partitions)
            {
                virtual_header.insert({p.type->createColumn(), p.type, p.name});
            }
            virtual_header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "_key"});

            /// Append "idx" column as the filter result
            //                virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});
            //
            //                auto block = virtual_header.cloneEmpty();
            //                addPathToVirtualColumns(block, fs::path(bucket) / keys.front(), 0);

            ASTPtr filter_ast;
            VirtualColumnUtils::prepareFilterBlockWithQuery(*query, local_context, virtual_header, filter_ast);

            if (filter_ast)
            {
                Block block = virtual_header.cloneEmpty();
                for (const auto & kv : metadata.key_with_partition_values)
                {
                    for (const auto & p : partitions)
                    {
                        block.getByName(p.name).column->assumeMutableRef().insert(kv.second->at(p.name));
                    }
                    block.getByName("_key").column->assumeMutableRef().insert(kv.first);
                }

                VirtualColumnUtils::filterBlockWithQuery(*query, virtual_header, local_context, filter_ast);
                const auto & keys_col = typeid_cast<const ColumnString &>(*block.getByName("_key").column);

                std::set<String> filtered_keys;

                for (size_t i = 0; i < keys_col.size(); i++)
                {
                    filtered_keys.insert(keys_col.getDataAt(i).data);
                }
                configuration.keys = {filtered_keys.begin(), filtered_keys.end()};
            }
        }

        LOG_TRACE(
            &Poco::Logger::get("DataLake"),
            "New configuration path: {}, keys: {}",
            configuration.getPath(), fmt::join(configuration.keys, ", "));

        configuration.connect(local_context);
        return configuration;
    }

    void updateConfigurationImpl(std::optional<ASTPtr> query, ContextPtr local_context)
    {
        const bool updated = base_configuration.update(local_context);
        if (!updated)
            return;

        Storage::useConfiguration(getConfigurationForDataRead(base_configuration, local_context, query));
    }

    Configuration base_configuration;
    std::mutex configuration_update_mutex;
    Poco::Logger * log;
};


template <typename DataLake>
static StoragePtr createDataLakeStorage(const StorageFactory::Arguments & args)
{
    auto configuration = DataLake::getConfiguration(args.engine_args, args.getLocalContext());

    /// Data lakes use parquet format, no need for schema inference.
    if (configuration.format == "auto")
        configuration.format = "Parquet";

    return std::make_shared<DataLake>(
        configuration, args.getContext(), args.table_id, args.columns, args.constraints,
        args.comment, getFormatSettings(args.getContext()));
}

}

#endif
