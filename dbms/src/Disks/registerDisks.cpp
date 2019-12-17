#include <Common/config.h>

#include "DiskFactory.h"

namespace DB
{
void registerDiskLocal(DiskFactory & factory);
#if USE_AWS_S3
void registerDiskS3(DiskFactory & factory);
#endif

void registerDisks()
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory);
#if USE_AWS_S3
    registerDiskS3(factory);
#endif
}

}
