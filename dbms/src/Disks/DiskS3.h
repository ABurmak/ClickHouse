#pragma once

#include <Disks/IDisk.h>

namespace DB
{
class DiskFileS3 : public IDiskFile
{
public:
    bool exists() const override;

    bool isDirectory() const override;

    void createDirectory() override {}

    void createDirectories() override {}

    void moveTo(const String & new_path) override;

    void copyTo(const String & new_path) override;

    std::unique_ptr<ReadBuffer> read() const override;

    std::unique_ptr<WriteBuffer> write() override;

private:
    DiskDirectoryIteratorImplPtr iterateDirectory() override;
};

class DiskDirectoryIteratorS3 : public IDiskDirectoryIteratorImpl
{
public:
    const String & name() const override;

    const DiskFilePtr & get() const override;

    void next() override;

    bool isValid() const override;
};

class DiskS3 : public IDisk
{
public:
    UInt64 getTotalSpace() const override;

    UInt64 getAvailableSpace() const override;

    DiskFilePtr file(const String & path) const override;
};

}
