package com.altinity.clickhouse.debezium.embedded.ddl.parser;

import com.altinity.clickhouse.debezium.embedded.parser.DataTypeConverter;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfigVariables;
import com.altinity.clickhouse.sink.connector.common.Utils;
import com.altinity.clickhouse.sink.connector.db.BaseDbWriter;
import com.altinity.clickhouse.sink.connector.db.DBMetadata;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.AlterByAddColumnContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.TableNameContext;
import io.debezium.relational.ddl.DataType;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;

import static com.altinity.clickhouse.sink.connector.db.ClickHouseDbConstants.*;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;


/**
 * This class contains the only overridden functions from the generated parser.
 */
public class MySqlDDLParserListenerImpl extends MySQLDDLParserBaseListener {
    private static final Logger log = LogManager.getLogger(MySqlDDLParserListenerImpl.class);
    StringBuffer query;
    String tableName;
    ClickHouseSinkConnectorConfig config;
    Map<String, String> sourceToDestinationMap = new HashMap<>();

    String databaseName;

    BaseDbWriter writer;

    DBMetadata dbMetadata;

    public MySqlDDLParserListenerImpl(BaseDbWriter writer, StringBuffer transformedQuery, String tableName,
                                      String databaseName,
                                      ClickHouseSinkConnectorConfig config) {
        this.config = config;
        try {
        if (this.config.getString(ClickHouseSinkConnectorConfigVariables.CLICKHOUSE_DATABASE_OVERRIDE_MAP.toString()) != null)
            sourceToDestinationMap = Utils.parseSourceToDestinationDatabaseMap(this.config.
                    getString(ClickHouseSinkConnectorConfigVariables.CLICKHOUSE_DATABASE_OVERRIDE_MAP.toString()));

        } catch(Exception e) {
            log.error("enterCreateDatabase: Error parsing source to destination database map:" + e.toString());
        }

        this.databaseName = overrideDatabaseName(databaseName);

        this.query = transformedQuery;
        this.tableName = tableName.toLowerCase();

        this.config = config;
        this.dbMetadata = new DBMetadata();
        this.writer = writer;
    }

    /**
     * Function to override the database name.
     * @param databaseName
     * @return
     */
    private String overrideDatabaseName(String databaseName) {

        // databaseName might contain backticks. Remove them.
        if(databaseName.contains("`")) {
            databaseName = databaseName.replace("`", "");
        }

        if(sourceToDestinationMap.containsKey(databaseName)) {
            return sourceToDestinationMap.get(databaseName);
        }
        return databaseName;
    }

    @Override
    public void enterCopyCreateTable(MySqlParser.CopyCreateTableContext copyCreateTableContext) {
        ListIterator<ParseTree> it = copyCreateTableContext.children.listIterator();

        String originalTableName = "";
        String newTableName = "";

        while(it.hasNext()) {
            ParseTree tree = it.next();
            if(tree instanceof MySqlParser.TableNameContext) {
                originalTableName = tree.getText().toLowerCase();
                if(it.next().getText().equalsIgnoreCase(Constants.LIKE)) {
                    newTableName = it.next().getText().toLowerCase();
                }
            }
        }
        if (originalTableName.contains(".")) {
            originalTableName = originalTableName.split("\\.")[1];
        }
        if (newTableName.contains(".")) {
            newTableName = newTableName.split("\\.")[1];
        }
        newTableName = newTableName.toLowerCase();
        originalTableName = originalTableName.toLowerCase();
        this.query.append(Constants.CREATE_TABLE).append(" ").append(databaseName).append(".").append(originalTableName).append(" ")
            .append(Constants.AS).append(" ").append(databaseName).append(".").append(newTableName);
    }

    @Override
    public void enterColumnCreateTable(MySqlParser.ColumnCreateTableContext columnCreateTableContext) {
        StringBuilder orderByColumns = new StringBuilder();
        StringBuilder partitionByColumn = new StringBuilder();
        parseCreateTable(columnCreateTableContext, orderByColumns, partitionByColumn);
        // Check if the destination is ReplicatedReplacingMergeTree.
        boolean isReplicatedReplacingMergeTree = config.getBoolean(ClickHouseSinkConnectorConfigVariables
                .AUTO_CREATE_TABLES_REPLICATED.toString());
        this.query.append("`").append(VERSION_COLUMN).append("` ").append(VERSION_COLUMN_DATA_TYPE).append(",");
        this.query.append("`").append(IS_DELETED_COLUMN).append("` ").append(IS_DELETED_COLUMN_DATA_TYPE);
        this.query.append(")");
        if(isReplicatedReplacingMergeTree == true) {
            this.query.append(String.format("Engine=ReplicatedReplacingMergeTree(%s, %s, %s)", CLUSTER_STR, VERSION_COLUMN, IS_DELETED_COLUMN));
        } else
            this.query.append(" Engine=ReplacingMergeTree(").append(VERSION_COLUMN).append(",").append(IS_DELETED_COLUMN).append(")");

        if(partitionByColumn.length() > 0) {
            this.query.append(Constants.PARTITION_BY).append(" ").append(partitionByColumn);
        }
        if(orderByColumns.length() == 0) {
        this.query.append(Constants.ORDER_BY_TUPLE);
        } else {
            this.query.append(Constants.ORDER_BY).append(orderByColumns.toString());
        }
        this.query.append(STORAGE_POLICY);
    }

    private void parseCreateTable(MySqlParser.CreateTableContext ctx, StringBuilder orderByColumns,
                                  StringBuilder partitionByColumns) {
        List<ParseTree> pt = ctx.children;
        Set<String> columnNames = new HashSet<>();

        this.query.append(Constants.CREATE_TABLE).append(" ");
        for (ParseTree tree : pt) {
            if (tree instanceof TableNameContext) {
                this.tableName = tree.getText().toLowerCase();
                if (tableName.contains(".")) {
                    tableName = tableName.split("\\.")[1];
                }
                this.query.append(databaseName).append(".").append(tableName);
                this.query.append("(");
            }else if (tree instanceof MySqlParser.CreateDefinitionsContext) {
                for (ParseTree subtree : ((MySqlParser.CreateDefinitionsContext) tree).children) {
                    if (subtree instanceof TerminalNodeImpl) {
                       // this.query.append(subtree.getText());
                    } else if (subtree instanceof MySqlParser.ColumnDeclarationContext) {
                        parseColumnDefinitions(subtree, orderByColumns, columnNames);
                    } else if(subtree instanceof MySqlParser.ConstraintDeclarationContext) {
                        for(ParseTree constraintTree: ((MySqlParser.ConstraintDeclarationContext) subtree).children) {
                            if(constraintTree instanceof MySqlParser.PrimaryKeyTableConstraintContext) {
                                for(ParseTree primaryKeyTree: ((MySqlParser.PrimaryKeyTableConstraintContext) constraintTree).children) {
                                    if(primaryKeyTree instanceof MySqlParser.IndexColumnNamesContext) {
                                        String primaryKeyColumns = primaryKeyTree.getText();
                                        if(primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
                                            orderByColumns.append(primaryKeyColumns);
                                        }
                                        //log.info("PRIMARY KEY");
                                    }

                                }
                            }
                        }
                    }
                }
            } else if(tree instanceof MySqlParser.PartitionDefinitionsContext) {
                for(ParseTree partitionTree: ((MySqlParser.PartitionDefinitionsContext) tree).children) {
                    if(partitionTree instanceof MySqlParser.PartitionFunctionKeyContext) {
                        for(ParseTree partitionKeyTree: ((MySqlParser.PartitionFunctionKeyContext) partitionTree).children) {
                            if(partitionKeyTree instanceof MySqlParser.UidListContext) {
                                String partitionColumn = partitionKeyTree.getText();
                                partitionByColumns.append(partitionColumn);
                            }
                        }

                    } else if(partitionTree instanceof MySqlParser.PartitionFunctionRangeContext) {
                        for(ParseTree partitionFunctionRangeTree: ((MySqlParser.PartitionFunctionRangeContext) partitionTree).children) {
                            if(partitionFunctionRangeTree instanceof MySqlParser.UidListContext) {
                                partitionByColumns.append("(").append(partitionFunctionRangeTree.getText()).append(")");
                            }
                        }

                    }
                }
            }
        }
    }

    /**
     * Function to parse column definitions.
     * @param subtree
     * @param orderByColumns
     *
     * @return list of column names
     */
    private void parseColumnDefinitions(ParseTree subtree, StringBuilder orderByColumns, Set<String> columnNames) {
        String columnName = null;
        String colDataType = null;
        boolean isNullColumn = true;
        boolean isGeneratedColumn = false;
        String generatedColumn = "";
        String columnComment = null;

        for (ParseTree colDefTree : ((MySqlParser.ColumnDeclarationContext) subtree).children) {
            if (colDefTree instanceof MySqlParser.FullColumnNameContext) {
                columnName = colDefTree.getText();
                this.query.append(columnName).append(" ");
            } else if (colDefTree instanceof MySqlParser.ColumnDefinitionContext) {
                String colDataTypeDefinition = colDefTree.getText();

                colDataType = getClickHouseDataType(colDataTypeDefinition, colDefTree, columnName);
                columnComment = null;
                // Null Column and DimensionDataType are children of ColumnDefinition
                for(ParseTree colDefinitionChildTree: ((MySqlParser.ColumnDefinitionContext) colDefTree).children) {
                    if (colDefinitionChildTree instanceof MySqlParser.NullColumnConstraintContext) {
                        if (colDefinitionChildTree.getText().equalsIgnoreCase(Constants.NOT_NULL))
                            isNullColumn = false;
                    } else if(colDefinitionChildTree instanceof MySqlParser.DimensionDataTypeContext) {
                        if (colDefinitionChildTree.getText() != null) {

                        }
                    } else if (colDefinitionChildTree instanceof MySqlParser.PrimaryKeyColumnConstraintContext) {
                        for(ParseTree primaryKeyTree: ((MySqlParser.PrimaryKeyColumnConstraintContext) colDefinitionChildTree).children) {
                            System.out.println(primaryKeyTree.getText());
                            orderByColumns.append(columnName);
                            break;
                        }
                    } else if (colDefinitionChildTree instanceof MySqlParser.GeneratedColumnConstraintContext) {
                        for(ParseTree generatedColumnTree: ((MySqlParser.GeneratedColumnConstraintContext) colDefinitionChildTree).children) {
                            if(generatedColumnTree instanceof MySqlParser.ExpressionContext) {
                                isGeneratedColumn = true;
                                generatedColumn = generatedColumnTree.getText();
                                //this.query.append(Constants.AS).append(" ").append(expression);
                            }
                        }

                    } else if (colDefinitionChildTree instanceof MySqlParser.CommentColumnConstraintContext) {
                        // COMMENT 'xxx' → ClickHouse列注释
                        TerminalNode commentNode = ((MySqlParser.CommentColumnConstraintContext) colDefinitionChildTree).STRING_LITERAL();
                        if (commentNode != null) {
                            columnComment = commentNode.getText();
                        }
                    }
                }
                // 列注释: ClickHouse语法 col Type COMMENT 'xxx'
                String commentClause = (columnComment == null) ? "" : " COMMENT " + columnComment;
                if(isGeneratedColumn) {
                    if(isNullColumn){
                        this.query.append(Constants.NULLABLE).append("(").append(colDataType)
                                .append(")");
                    } else
                        this.query.append(colDataType);

                    this.query.append(commentClause).append(" ").append(Constants.ALIAS).append(" ").append(generatedColumn).append(",");
                    continue;
                }

                // Nullable should not be added for POINT data type
                String lowerCaseDataType = colDataType.toLowerCase();
                if(!Constants.NULLABLE_NOT_SUPPORTED_DATA_TYPES.contains(lowerCaseDataType) && isNullColumn) {
                    this.query.append(Constants.NULLABLE).append("(").append(colDataType)
                            .append(")").append(commentClause).append(",");
                }
                else {
                    // 非nullable列不输出NOT NULL修饰符(ClickHouse列默认NOT NULL, 部分解析器不支持该语法)
                    this.query.append(colDataType).append(commentClause).append(",");
                }
                columnNames.add(columnName);
            }
        }
    }

    /**
     * Function to get the ClickHouse Data type from DDL Datatype.
     * @param parsedDataType
     * @return
     */
    private String getClickHouseDataType(String parsedDataType, ParseTree colDefTree, String columnName) {
        int precision = 0;
        int scale = 0;

        String chDataType = null;
        MySqlParser.DataTypeContext dtc = ((MySqlParser.ColumnDefinitionContext) colDefTree).dataType();
        DataType dt = DataTypeConverter.getDataType(dtc);

        if(dt.name().equalsIgnoreCase("ENUM"))
        {
            // Dont try to get precision/scale for enums
        }
        else if(parsedDataType.contains("(") && parsedDataType.contains(")") && parsedDataType.contains(",") ) {
            try {
                precision = Integer.parseInt(parsedDataType.substring(parsedDataType.indexOf("(") + 1, parsedDataType.indexOf(",")));
                scale = Integer.parseInt(parsedDataType.substring(parsedDataType.indexOf(",") + 1, parsedDataType.indexOf(")")));
            } catch(Exception e) {
                log.error("Error parsing precision, scale : columnName" + columnName);
            }
        }  // datetime(6)
        else if(parsedDataType.contains("(") && parsedDataType.contains(")") &&
                (containsIgnoreCase(parsedDataType, "datetime") || containsIgnoreCase(parsedDataType, "timestamp"))){
            try {
                precision = Integer.parseInt(parsedDataType.substring(parsedDataType.indexOf("(") + 1, parsedDataType.indexOf(")")));
            } catch(Exception e) {
                log.error("Error parsing precision:ColumnName:" + columnName);
            }
        }

        chDataType = DataTypeConverter.convertToString(this.config, columnName,
                scale, precision, dtc);

        return chDataType;

    }


    private void parseAddIndex(ParseTree tree) {

        // add index col3_index(col3) TYPE minmax GRANULARITY 4;
        for (ParseTree columnChild : ((MySqlParser.AlterByAddIndexContext) tree).children) {

            if (columnChild instanceof MySqlParser.IfNotExistsContext) {

            } else if (columnChild instanceof MySqlParser.UidContext) {
                // Name of index.

            } else if (columnChild instanceof MySqlParser.IndexTypeContext) {
                // Index type

            } else if (columnChild instanceof MySqlParser.IndexColumnNamesContext) {
                for (ParseTree columnNameChild : ((MySqlParser.IndexColumnNamesContext) (columnChild)).children) {
                    // Column Name
                }

            } else if (columnChild instanceof MySqlParser.IndexOptionContext) {
                // Index option
                // comment
            }
        }
    }

    private void parseRenameColumn(ParseTree tree) {
        ListIterator<ParseTree> it = ((MySqlParser.AlterSpecificationContext) tree).children.listIterator();
        // this.query.append(" ").append(Constants.RENAME_COLUMN);
        //for (ParseTree columnChild : ((MySqlParser.AlterSpecificationContext) tree).children) {
        while (it.hasNext()) {
            ParseTree child = it.next();
            if (child instanceof MySqlParser.UidContext) {
                this.query.append(" ").append(child.getText());
            } else if (child instanceof TerminalNodeImpl) {
                this.query.append(" ").append(child.getText());
            }
        }
    }

    private void parseAlterTable(ParseTree tree) {

        String columnName = null;
        String columnType = null;
        String newColumnName = null;

        String modifier = Constants.ADD_COLUMN;
        String modifierWithNull = Constants.ADD_COLUMN_NULLABLE;

        String defaultModifier = null;

        StringBuffer columnPositionModifier = new StringBuffer();

        boolean isNullColumn = false;
        boolean isAlterChangeColumn = false;
        boolean nullExplicitlySet = false;

        if (tree instanceof AlterByAddColumnContext) {
            modifier = Constants.ADD_COLUMN;
            modifierWithNull = Constants.ADD_COLUMN_NULLABLE;
            isNullColumn = true;

        } else if (tree instanceof MySqlParser.AlterByModifyColumnContext) {
            modifier = Constants.MODIFY_COLUMN;
            modifierWithNull = Constants.MODIFY_COLUMN_NULLABLE;
        } else if (tree instanceof MySqlParser.AlterByRenameColumnContext) {
            modifier = Constants.RENAME_COLUMN;
            modifierWithNull = Constants.RENAME_COLUMN_NULLABLE;

        } else if (tree instanceof MySqlParser.AlterByChangeColumnContext) {
            isAlterChangeColumn = true;
            modifier = Constants.MODIFY_COLUMN;
            modifierWithNull = Constants.MODIFY_COLUMN_NULLABLE;
        } else if (tree instanceof MySqlParser.AlterByAddIndexContext) {
            modifier = Constants.ADD_INDEX;
        } else {
            //log.error("Not support Alter specification context");
            return;
        }
        ListIterator<ParseTree> it = ((MySqlParser.AlterSpecificationContext) tree).children.listIterator();

        //for (ParseTree columnChild : ((MySqlParser.AlterSpecificationContext) tree).children) {
        while (it.hasNext()) {
            ParseTree columnChild = it.next();
            if (columnChild instanceof MySqlParser.UidContext) {
                columnName = (columnChild).getText();
                if (isAlterChangeColumn) {
                    // Change column comes in this format ALTER TABLE change column oldcol newcol.
                    ParseTree newColumnChild = it.next();
                    newColumnName = newColumnChild.getText();
                }
            } else if (columnChild instanceof MySqlParser.ColumnDefinitionContext) {
                String colDataTypeDefinition = (columnChild.getText());
                columnType = getClickHouseDataType(colDataTypeDefinition, columnChild, columnName);
                for (ParseTree columnDefChild : ((MySqlParser.ColumnDefinitionContext) columnChild).children) {
                    if (columnDefChild instanceof MySqlParser.NullColumnConstraintContext) {
                        nullExplicitlySet = true;
                        if (columnDefChild.getText().equalsIgnoreCase(Constants.NULL))
                            isNullColumn = true;
                        else if(columnDefChild.getText().equalsIgnoreCase(Constants.NOT_NULL)) {
                            if(!modifier.equalsIgnoreCase(Constants.ADD_COLUMN)) {
                                isNullColumn = false;
                            }
                        }
                    } else if (columnDefChild instanceof MySqlParser.DefaultColumnConstraintContext) {
                        if (columnDefChild.getChildCount() >= 2) {
                            defaultModifier = "DEFAULT " + columnDefChild.getChild(1).getText();
                        }
                    }
                }
            } else if (columnChild instanceof TerminalNodeImpl) {
                String columnPosition = columnChild.getText();
                if (columnPosition.equalsIgnoreCase(Constants.AFTER)) {
                    // The next element in the tree will be the column
                    if (it.hasNext()) {
                        columnPositionModifier.append(columnPosition).append(" ").append(it.next().getText());
                    }
                } else if (columnPosition.equalsIgnoreCase(Constants.FIRST)) {
                    columnPositionModifier.append(columnPosition);
                }
                // columnName = columnName + " " + columnChild.getText();

            }
        }
        // if null is not explicitly set.
        if (!nullExplicitlySet) {
            try {
                if(writer == null) {
                    log.error("Error with DB connection");
                    throw new SQLException("Error with DB connection");
                }
                else {
                    Map<String, Boolean> isNullableList = dbMetadata.getColumnsIsNullableForTable(tableName, writer.getConnection(), databaseName);
                    if (isNullableList.get(columnName) != null && isNullableList.get(columnName)) {
                        isNullColumn = true;
                    } else if (isNullableList.get(columnName) == null) {
                        isNullColumn = true;
                    } else {
                        isNullColumn = false;
                    }
                }
            } catch (Exception e) {
                log.error("Error retrieving NULL column schema from ClickHouse", e);
            }
            // Check if the column scehma is nullable from ClickHouse.
                // Map<String, Boolean> isNullableList = dbMetadata.getColumnsIsNullableForTable(tableName, writer.getConnection(), databaseName);
        }
        if (columnName != null && columnType != null)
            if (isNullColumn) {
                this.query.append(" ").append(String.format(modifierWithNull, columnName, columnType)).append(" ");
            }
            else
                this.query.append(" ").append(String.format(modifier, columnName, columnType));

        if (defaultModifier != null && defaultModifier.isEmpty() == false) {
            this.query.append(" ").append(defaultModifier);
        }
        if (columnPositionModifier.length() != 0) {
            this.query.append(" ").append(columnPositionModifier);
        }

        if (isAlterChangeColumn && columnName != null && !columnName.equals(newColumnName)) {
            postProcessModifyColumn(this.tableName, columnName, newColumnName, columnType);
        }

        String trimmedQuery = this.query.toString().trim();
        this.query.delete(0, this.query.toString().length()).append(trimmedQuery);
    }

    /**
     * Function to create MODIFY column to rename the column name
     *
     * @param tableName
     * @param oldCol
     * @param newCol
     * @param dataType
     */
    public void postProcessModifyColumn(String tableName, String oldCol, String newCol, String dataType) {
        this.query.append("\n");
        if (tableName.contains(".")) {
            tableName = tableName.split("\\.")[1];
        }
        // If the tableName already includes the databaseName dont include databaseName in this.query
        this.query.append(String.format("ALTER TABLE %s RENAME COLUMN %s to %s", databaseName + "." + tableName, oldCol, newCol));

    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext alterTableContext) {


        List<ParseTree> pt = alterTableContext.children;
        for (ParseTree tree : pt) {

            if (tree instanceof TableNameContext) {
                this.tableName = tree.getText().toLowerCase();
                // If the table name already include the database name dont include it in the query.
                if(this.tableName.contains(".")) {
                    // Split database and table name.
                    String[] tableNameSplit = this.tableName.split("\\.");
                
                    this.query.append(String.format(Constants.ALTER_TABLE, databaseName+ "." + tableNameSplit[1]));
                } else
                    this.query.append(String.format(Constants.ALTER_TABLE, databaseName + "." + this.tableName));
            }

            if (tree instanceof AlterByAddColumnContext) {
                parseAlterTable(tree);

            }
            else if (tree instanceof MySqlParser.AlterByDropConstraintCheckContext) {
                // Drop Constraint.
                this.query.append(" ");
                for (ParseTree dropConstraintTree : ((MySqlParser.AlterByDropConstraintCheckContext) (tree)).children) {
                    if (dropConstraintTree instanceof MySqlParser.UidContext) {
                        System.out.println("Drop Constraint");
                        this.query.append(String.format(Constants.DROP_CONSTRAINT, dropConstraintTree.getText()));
                    }
                }
            }
            else if (tree instanceof MySqlParser.AlterByModifyColumnContext) {
                parseAlterTable(tree);
            } else if (tree instanceof MySqlParser.AlterByDropColumnContext) {
                // Drop Column.
                this.query.append(" ");
                for (ParseTree dropColumnTree : ((MySqlParser.AlterByDropColumnContext) (tree)).children) {
                    if (dropColumnTree instanceof MySqlParser.UidContext) {
                        for(ParseTree dropColumnChild: ((MySqlParser.UidContext) dropColumnTree).children) {
                            if(dropColumnChild instanceof MySqlParser.SimpleIdContext || dropColumnChild instanceof TerminalNodeImpl) {
                                this.query.append(String.format(Constants.DROP_COLUMN, dropColumnChild.getText()));
                            }
                        }
                       // this.query.append(String.format(Constants.DROP_COLUMN, ((MySqlParser.AlterByDropColumnContext) tree).uid()));
                    }
                }
            } else if (tree instanceof MySqlParser.AlterByRenameColumnContext) {
                parseRenameColumn(tree);
            } else if (tree instanceof MySqlParser.AlterByAddPrimaryKeyContext) {
                parseAlterTable(tree);
            } else if (tree instanceof MySqlParser.AlterByChangeColumnContext) {
                parseAlterTable(tree);
            } else if (tree instanceof MySqlParser.AlterByAddIndexContext) {
                parseAddIndex(tree);
            } else if(tree instanceof MySqlParser.AlterBySetAlgorithmContext) {
                log.info("INSTANT ALGORITHM not supported in ClickHouse");
                // Remove any terminating commas and break out of the parser loop.
                // If the last character was comma.
                if(this.query.charAt(this.query.length() - 1) == ',')
                    this.query.deleteCharAt(this.query.length() - 1);

                break;
            }
            else if (tree instanceof TerminalNodeImpl) {
                if (((TerminalNodeImpl) tree).symbol.getType() == MySqlParser.COMMA) {
                    this.query.append(",");
                }
            } else if(tree instanceof MySqlParser.AlterByRenameContext) {
                parseAlterTableByRename(tableName, (MySqlParser.AlterByRenameContext) tree);
            }
        }
    }

    private void parseAlterTableByRename(String originalTableName, MySqlParser.AlterByRenameContext tree) {
        String newTableName = null;
        for(ParseTree alterByRenameChildren: tree.children) {
            if(alterByRenameChildren instanceof MySqlParser.UidContext) {
                newTableName = alterByRenameChildren.getText();
            } else if(alterByRenameChildren instanceof MySqlParser.FullIdContext) {
                newTableName = alterByRenameChildren.getText();
            }
        }
        if (originalTableName.contains(".")) {
            originalTableName = originalTableName.split("\\.")[1];
        }
        if (newTableName.contains(".")) {
            newTableName = newTableName.split("\\.")[1];
        }
        newTableName = newTableName.toLowerCase();
        originalTableName = originalTableName.toLowerCase();
        // If the databasename already includes the table name dont include it in the query.
        this.query.delete(0, this.query.toString().length()).append(String.format
            (Constants.ALTER_RENAME_TABLE, databaseName + "." + originalTableName, databaseName + "." + newTableName));

    }

    @Override
    public void enterAlterByAddCheckTableConstraint(MySqlParser.AlterByAddCheckTableConstraintContext alterByAddCheckTableConstraintContext) {
        // log.info("Enter check table constraint: " + alterByAddCheckTableConstraintContext.getText() );
        this.query.append(" ");
        for (ParseTree tree : alterByAddCheckTableConstraintContext.children) {
            this.parseTreeHelper(tree);
        }
    }

    private void parseTreeHelper(ParseTree child) {
        if (child instanceof MySqlParser.UidContext) {
            this.query.append(child.getText()).append(" ");
        } else if (child instanceof MySqlParser.ComparisonOperatorContext) {
            this.query.append(child.getText());
        } else if (child instanceof TerminalNodeImpl) {
            this.query.append(child.getText()).append(" ");
        } else if (child instanceof ParserRuleContext) {
            for (ParseTree child2 : ((ParserRuleContext) child).children) {
                this.parseTreeHelper(child2);
            }
        }
    }

//    @Override
//    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext alterByDropColumnContext) {
//        this.query.append(" ");
//        for (ParseTree tree : alterByDropColumnContext.children) {
//            if (tree instanceof MySqlParser.UidContext) {
//                this.query.append(String.format(Constants.DROP_COLUMN, tree.getText()));
//            }
//        }
//    }

    @Override
    public void enterDropTable(MySqlParser.DropTableContext dropTableContext) {
        log.debug("DROP TABLE enter");
        String tableName;
        this.query.append(Constants.DROP_TABLE).append(" ");
        for (ParseTree child : dropTableContext.children) {
            if (child instanceof MySqlParser.TablesContext) {
                for (ParseTree tableNameChild : ((MySqlParser.TablesContext) child).children) {
                    if (tableNameChild instanceof MySqlParser.TableNameContext) {
                        //this.query.append(tableNameChild.getText().toLowerCase());
                        tableName = tableNameChild.getText().toLowerCase();
                        if (tableName.contains(".")) {
                            tableName = tableName.split("\\.")[1];
                        }
                        this.query.append(databaseName).append(".").append(tableName);
                    } else if (tableNameChild instanceof TerminalNodeImpl) {
                        //this.query.append(tableNameChild.getText().toLowerCase());
                        tableName = tableNameChild.getText().toLowerCase();
                        if (tableName.contains(".")) {
                            tableName = tableName.split("\\.")[1];
                        }
                        this.query.append(databaseName).append(".").append(tableName);
                    }
                }
            }
        }
    }


    @Override
    public void enterRenameTable(MySqlParser.RenameTableContext renameTableContext) {
        this.query.append(Constants.RENAME_TABLE).append(" ");
        String originalTableName = null;
        String newTableName = null;
        for (ParseTree child : renameTableContext.children) {
            if (child instanceof MySqlParser.RenameTableClauseContext) {
                List<ParseTree> renameTableContextChildren = ((MySqlParser.RenameTableClauseContext) child).children;

                if (renameTableContextChildren.size() >= 3) {
                    originalTableName = renameTableContextChildren.get(0).getText().toLowerCase();
                    newTableName = renameTableContextChildren.get(2).getText().toLowerCase();
                    if (originalTableName.contains(".")) {
                        originalTableName = originalTableName.split("\\.")[1];
                    }
                    if (newTableName.contains(".")) {
                        newTableName = newTableName.split("\\.")[1];
                    }
                    // If the table name already includes the database name dont include it in the query.
                    this.query.append(databaseName).append(".").append(originalTableName).append(" to ").
                            append(databaseName).append(".").append(newTableName);
                }
            } else if(child instanceof TerminalNodeImpl) {
                if (((TerminalNodeImpl) child).symbol.getType() == MySqlParser.COMMA) {
                    this.query.append(",");
                }
            }
        }
    }

    @Override
    public void enterTruncateTable(MySqlParser.TruncateTableContext truncateTableContext) {
        for (ParseTree child : truncateTableContext.children) {
            if (child instanceof MySqlParser.TableNameContext) {
                String tableName = child.getText().toLowerCase();
                if (tableName.contains(".")) {
                    tableName = tableName.split("\\.")[1];
                }
                this.query.append(String.format(Constants.TRUNCATE_TABLE, databaseName + "." + tableName));
            }
        }
    }
}
