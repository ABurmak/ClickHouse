#pragma once

#include <Common/config.h>

#if USE_AWS_S3
#    include <aws/s3/S3Client.h>
#    include <Poco/DirectoryIterator.h>
#    include "DiskFactory.h"


namespace DB
{
class DiskS3 : public IDisk
{
public:
    friend class DiskS3Reservation;

    DiskS3(
        const String & name_,
        std::shared_ptr<Aws::S3::S3Client> client_,
        const String & bucket_,
        const String & s3_root_path_,
        const String & metadata_path_);

    const String & getName() const override { return name; }

    const String & getPath() const override { return s3_root_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
        {
            remove(path + it->name());
        }
    }

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void copyFile(const String & from_path, const String & to_path) override;

    std::unique_ptr<ReadBuffer> readFile(const String & path, size_t buf_size) const override;

    std::unique_ptr<WriteBuffer> writeFile(const String & path, size_t buf_size, WriteMode mode) override;

    void remove(const String & path) override;

private:
    bool tryReserve(UInt64 bytes);

    String getS3Path(const String & path) const;

private:
    const String name;
    std::shared_ptr<Aws::S3::S3Client> client;
    String bucket;
    const String s3_root_path;
    const String metadata_path;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
};

using DiskS3Ptr = std::shared_ptr<DiskS3>;

class DiskS3DirectoryIterator : public IDiskDirectoryIterator
{
public:
    explicit DiskS3DirectoryIterator(const String & path) : iter(path) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != Poco::DirectoryIterator(); }

    String name() const override { return iter.name(); }

private:
    Poco::DirectoryIterator iter;
};

class DiskS3Reservation : public IReservation
{
public:
    DiskS3Reservation(const DiskS3Ptr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk() const override { return disk; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(IDisk::reservationMutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskS3Reservation() override;

private:
    DiskS3Ptr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

}

#endif
