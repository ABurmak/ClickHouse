#include <thread>

#include <Common/ThreadPool.h>

#include <Poco/DirectoryIterator.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>

#include <Databases/DatabaseOrdinary.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/escapeForFileName.h>

#include <Common/typeid_cast.h>


namespace DB
{

static void executeCreateQuery(
    const String & query,
    Context & context,
    const String & database,
    const String & file_name,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name, 0);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.attach = true;
    ast_create_query.database = database;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
    interpreter.execute();
}


static void loadDatabaseImpl(
    Context & context,
    const String & database,
    const String & database_attach_query,
    const String & metadata_path,
    bool force_restore_data)
{
    /// Use database with default engine if .sql file with database attach query is absent.

    String attach_query = database_attach_query;
    if (attach_query.empty())
        attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database);

    executeCreateQuery(database_attach_query, context, database, metadata_path, force_restore_data);
}


static void loadDatabase(
    Context & context,
    const String & database,
    const String & metadata_path,
    bool force_restore_data)
{
    String database_attach_query;
    if (Poco::File(metadata_path).exists())
    {
        ReadBufferFromFile in(metadata_path, 1024);
        readStringUntilEOF(database_attach_query, in);
    }

    loadDatabaseImpl(context, database, database_attach_query, metadata_path, force_restore_data);
}

static void loadDatabase(
    Context & context,
    const String & database,
    const DiskFilePtr & metadata_file,
    bool force_restore_data)
{
    String database_attach_query;
    if (metadata_file->exists())
    {
        auto in = metadata_file->read();
        readStringUntilEOF(database_attach_query, *in);
    }

    loadDatabaseImpl(context, database, database_attach_query, metadata_file->fullPath(), force_restore_data);
}


#define SYSTEM_DATABASE "system"


void loadMetadata(Context & context)
{
    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
    bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

    auto & disk = context.getDefaultDisk();
    auto metadata_dir = disk->file("metadata");

    /// Loop over databases.
    std::map<String, DiskFilePtr> databases;
    DiskDirectoryIterator dir_end;
    for (DiskDirectoryIterator it(metadata_dir); it != dir_end; ++it)
    {
        if (!it->isDirectory())
            continue;

        /// For '.svn', '.gitignore' directory and similar.
        if (it.name().at(0) == '.')
            continue;

        if (it.name() == SYSTEM_DATABASE)
            continue;

        auto metadata_file = disk->file("metadata/" + it.name() + ".sql");
        databases.emplace(unescapeForFileName(it.name()), metadata_file);
    }

    for (const auto & [name, metadata_file] : databases)
        loadDatabase(context, name, metadata_file, has_force_restore_data_flag);

    if (has_force_restore_data_flag)
    {
        try
        {
            force_restore_data_flag_file.remove();
        }
        catch (...)
        {
            tryLogCurrentException("Load metadata", "Can't remove force restore file to enable data sanity checks");
        }
    }
}


void loadMetadataSystem(Context & context)
{
    String path = context.getPath() + "metadata/" SYSTEM_DATABASE;
    if (Poco::File(path).exists())
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, SYSTEM_DATABASE, path, true);
    }
    else
    {
        /// Initialize system database manually
        String global_path = context.getPath();
        Poco::File(global_path + "data/" SYSTEM_DATABASE).createDirectories();
        Poco::File(global_path + "metadata/" SYSTEM_DATABASE).createDirectories();

        auto system_database = std::make_shared<DatabaseOrdinary>(SYSTEM_DATABASE, global_path + "metadata/" SYSTEM_DATABASE ".sql", context);
        context.addDatabase(SYSTEM_DATABASE, system_database);
    }

}

}
