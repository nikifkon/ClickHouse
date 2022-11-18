#pragma once
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/**
 * Create a DiskPtr from disk AST function like disk(<disk_configuration>),
 * add it to DiskSelector by a unique (but always the same for given configuration) disk name
 * and return this name.
 */
std::string createDiskFromDiskAST(const ASTFunction & function, ContextPtr context);

/*
 * Is given ast has form of a disk(<disk_configuration>) function.
 */
bool isDiskFunction(ASTPtr ast);

}
