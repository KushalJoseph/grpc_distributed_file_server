#pragma once

#include <cstdio>
#include <cstdlib>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"

void metaserver_api_initialize();

void metaserver_api_create(const char *filename, int stripe_width);
