/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */


#include "harness.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "common/common.h"
#include "execution/VoltDBEngine.h"
#include "storage/table.h"

#include <cstdlib>

using namespace voltdb;
using namespace std;

class AddDropTableTest : public Test {

  public:
    AddDropTableTest()
        : m_clusterId(0), m_databaseId(0), m_siteId(0), m_partitionId(0),
          m_hostId(101), m_hostName("host101"), m_catVersion(0)
    {
        m_engine = new VoltDBEngine();
        m_engine->initialize(m_clusterId, m_siteId, m_partitionId,
                             m_hostId, m_hostName);

        std::string initialCatalog =
          "add / clusters cluster\n"
          "add /clusters[cluster] databases database\n"
          "add /clusters[cluster]/databases[database] programs program\n"
          "add /clusters[cluster] hosts 0\n"
          "add /clusters[cluster] partitions 0\n"
          "add /clusters[cluster] partitions 1\n"
          "add /clusters[cluster] partitions 2\n"
          "add /clusters[cluster] sites 0\n"
          "set /clusters[cluster]/sites[0] partition /clusters[cluster]/partitions[0]\n"
          "set /clusters[cluster]/sites[0] host /clusters[cluster]/hosts[0]";

        bool loadResult = m_engine->loadCatalog(initialCatalog);
        ASSERT_TRUE(loadResult);
    }

    ~AddDropTableTest()
    {
        delete m_engine;
    }


    std::string tableACmds()
    {
        return
          "add /clusters[cluster]/databases[database] tables tableA\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] type 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] isreplicated false\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] partitioncolumn 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA] estimatedtuplecount 0\n"
          "add /clusters[cluster]/databases[database]/tables[tableA] columns A\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] index 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] type 5\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] size 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] nullable false\n"
          "set /clusters[cluster]/databases[database]/tables[tableA]/columns[A] name \"A\"";
    }

    std::string tableADeleteCmd()
    {
        return "delete /clusters[cluster]/databases[database] tables tableA";
    }

    std::string tableBCmds()
    {
        return
          "add /clusters[cluster]/databases[database] tables tableB\n"
          "set /clusters[cluster]/databases[database]/tables[tableB] type 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableB] isreplicated false\n"
          "set /clusters[cluster]/databases[database]/tables[tableB] partitioncolumn 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableB] estimatedtuplecount 0\n"
          "add /clusters[cluster]/databases[database]/tables[tableB] columns A\n"
          "set /clusters[cluster]/databases[database]/tables[tableB]/columns[A] index 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableB]/columns[A] type 5\n"
          "set /clusters[cluster]/databases[database]/tables[tableB]/columns[A] size 0\n"
          "set /clusters[cluster]/databases[database]/tables[tableB]/columns[A] nullable false\n"
          "set /clusters[cluster]/databases[database]/tables[tableB]/columns[A] name \"A\"";
    }

    std::string tableBDeleteCmd()
    {
        return "delete /clusters[cluster]/databases[database] tables tableB";
    }


  protected:
    CatalogId m_clusterId;
    CatalogId m_databaseId;
    CatalogId m_siteId;
    CatalogId m_partitionId;
    CatalogId m_hostId;
    std::string m_hostName;
    int m_catVersion;         // catalog version
    VoltDBEngine *m_engine;
};

/*
 * Test on catalog.
 * Verify new table has the add flag set.
 */
TEST_F(AddDropTableTest, DetectNewTable)
{
    // add a table to voltdbengine's catalog
    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog->execute(tableACmds());

    bool found = false;
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    // get the table and see that is newly added
    // also assert that it really exists in the new catalog.
    map<string, catalog::Table*>::const_iterator t_iter;
    for (t_iter = db->tables().begin();
         t_iter != db->tables().end();
         t_iter++)
    {
        catalog::Table *t = t_iter->second;
        if (t->name() == "tableA") {
            ASSERT_TRUE(t->wasAdded());
            found = true;
        }
        else {
            ASSERT_FALSE(t->wasAdded());
        }
    }
    ASSERT_TRUE(found);
}

/*
 * Test on catalog.
 * Delete a table and make sure it is absent.
 */
TEST_F(AddDropTableTest, DetectDeletedTable)
{
    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog->execute(tableACmds());

    bool found = false;
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    // delete the table and verify its absence
    catalog->execute(tableADeleteCmd());
    map<string, catalog::Table*>::const_iterator t_iter;
    for (found = false, t_iter = db->tables().begin();
         t_iter != db->tables().end();
         t_iter++)
    {
        catalog::Table *t = t_iter->second;
        if (t->name() == "tableA") {
            found = true;
        }
        else {
            ASSERT_FALSE(t->wasAdded());
        }
    }
    ASSERT_FALSE(found);

    // verify tableA appears in the deletion list
    vector<string> deletions;
    catalog->getDeletedPaths(deletions);
    vector<string>::iterator delIter;
    delIter = deletions.begin();
    found = false;
    while (delIter != deletions.end()) {
        string item = *delIter;
        if (item == "/clusters[cluster]/databases[database]/tables[tableA]") {
            found = true;
        }
        ++delIter;
    }
    ASSERT_TRUE(found);
}

/*
 * Test on catalog.
 * Verify that subsequent execute() calls clear the wasAdded flags
 * from previous execute() calls.
 */
TEST_F(AddDropTableTest, WasAddedFlagCleared)
{
    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog->execute(tableACmds());
    catalog->execute(tableBCmds());

    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    ASSERT_EQ(2, db->tables().size());

    map<string, catalog::Table*>::const_iterator t_iter;
    for (t_iter = db->tables().begin();
         t_iter != db->tables().end();
         t_iter++)
    {
        catalog::Table *t = t_iter->second;
        if (t->name() == "tableA") {
            ASSERT_FALSE(t->wasAdded());
        }
        else if (t->name() == "tableB") {
            ASSERT_TRUE(t->wasAdded());
        }
    }
}

TEST_F(AddDropTableTest, DeletionsSetCleared)
{
    vector<std::string> deletions;
    vector<std::string>::iterator delIter;

    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");

    catalog->execute(tableACmds());
    catalog->execute(tableBCmds());

    // delete a table. verify deletion bookkeeping
    catalog->execute(tableADeleteCmd());
    ASSERT_EQ(1, db->tables().size());
    catalog->getDeletedPaths(deletions);
    ASSERT_EQ(1, deletions.size());
    delIter = deletions.begin();
    while (delIter != deletions.end()) {
        string path = *delIter;
        ASSERT_EQ(path, "/clusters[cluster]/databases[database]/tables[tableA]");
        ++delIter;
    }

    // delete a second table. verify deletion bookkeeping
    catalog->execute(tableBDeleteCmd());
    ASSERT_EQ(0, db->tables().size());
    deletions.clear();
    catalog->getDeletedPaths(deletions);
    ASSERT_EQ(1, deletions.size());
    delIter = deletions.begin();
    while (delIter != deletions.end()) {
        string path = *delIter;
        ASSERT_EQ(path, "/clusters[cluster]/databases[database]/tables[tableB]");
        ++delIter;
    }
}

/*
 * Test on engine.
 * Verify updateCatalog adds table to engine's collections.
 */
TEST_F(AddDropTableTest, AddTable)
{
    bool changeResult = m_engine->updateCatalog(tableACmds(), ++m_catVersion);
    ASSERT_TRUE(changeResult);

    Table *table1, *table2;
    table1 = m_engine->getTable("tableA");
    ASSERT_TRUE(table1 != NULL);

    table2 = m_engine->getTable(1); // catalogId
    ASSERT_TRUE(table2 != NULL);
    ASSERT_TRUE(table1 == table2);
}

/*
 * Test on engine.
 * Add two tables at once!
 */
TEST_F(AddDropTableTest, AddTwoTablesDropTwoTables)
{
    Table *table1, *table2;

    catalog::Catalog *catalog = m_engine->getCatalog();
    catalog::Cluster *cluster = catalog->clusters().get("cluster");
    catalog::Database *db = cluster->databases().get("database");
    ASSERT_EQ(0, db->tables().size());

    // add tableA, tableB
    std::string a_and_b = tableACmds() + "\n" + tableBCmds();
    bool changeResult = m_engine->updateCatalog(a_and_b, ++m_catVersion);
    ASSERT_TRUE(changeResult);
    ASSERT_EQ(2, db->tables().size());

    // verify first table
    table1 = m_engine->getTable("tableA");
    ASSERT_TRUE(table1 != NULL);

    table2 = m_engine->getTable(1); // catalogId
    ASSERT_TRUE(table2 != NULL);
    ASSERT_TRUE(table1 == table2);

    // verify second table
    table1 = m_engine->getTable("tableB");
    ASSERT_TRUE(table1 != NULL);

    table2 = m_engine->getTable(2); // catalogId
    ASSERT_TRUE(table2 != NULL);
    ASSERT_TRUE(table1 == table2);

    // drop tableA, tableB and verify
    table1->incrementRefcount();
    table2->incrementRefcount();

    std::string drop = tableADeleteCmd() + "\n" + tableBDeleteCmd();
    changeResult = m_engine->updateCatalog(drop, ++m_catVersion);
    ASSERT_TRUE(changeResult);
    ASSERT_EQ(0, db->tables().size());
    ASSERT_EQ(NULL, m_engine->getTable(1)); // catalogId
    ASSERT_EQ(NULL, m_engine->getTable("tableA"));
    ASSERT_EQ(NULL, m_engine->getTable(2)); // catalogId
    ASSERT_EQ(NULL, m_engine->getTable("tableB"));

    table1->decrementRefcount();
    table2->decrementRefcount();
}

/*
 * Test on engine.
 * Verify updateCatalog removes a table from engine's collections.
 */
TEST_F(AddDropTableTest, DropTable)
{
    // add. verified by AddTable test.
    bool result = m_engine->updateCatalog(tableACmds(), ++m_catVersion);
    ASSERT_TRUE(result);

    Table *table1, *table2;

    // grab the table. need some data from it to complete the
    // test. hold a reference to keep it safe.
    table1 = m_engine->getTable("tableA");
    table1->incrementRefcount();

    ASSERT_TRUE(table1 != NULL);

    // and delete
    result = m_engine->updateCatalog(tableADeleteCmd(), ++m_catVersion);
    ASSERT_TRUE(result);

    table2 = m_engine->getTable("tableA");
    ASSERT_TRUE(table2 == NULL);

    table2 = m_engine->getTable(0);
    ASSERT_TRUE(table2 == NULL);

    // release the last reference.
    table1->decrementRefcount();
}

/*
 * Test on engine.
 * Remove a non-existent table.
 */
TEST_F(AddDropTableTest, BadDropTable)
{
    bool result;
    result = m_engine->updateCatalog(tableACmds(), ++m_catVersion);
    ASSERT_TRUE(result);

    try {
        result = m_engine->updateCatalog(tableBDeleteCmd(), ++m_catVersion);
        ASSERT_TRUE(false);
    }
    catch (SerializableEEException ex) {
        ASSERT_TRUE(true);
    }
}

/*
 * Test on engine.
 * Add a table twice.
 */
TEST_F(AddDropTableTest, BadAddTable)
{
    bool result;
    result = m_engine->updateCatalog(tableACmds(), ++m_catVersion);
    ASSERT_TRUE(result);

    try {
        result = m_engine->updateCatalog(tableACmds(), ++m_catVersion);
        ASSERT_TRUE(false);
    }
    catch (SerializableEEException ex) {
        ASSERT_TRUE(true);
    }
}


int main() {
    return TestSuite::globalInstance()->runAll();
}

