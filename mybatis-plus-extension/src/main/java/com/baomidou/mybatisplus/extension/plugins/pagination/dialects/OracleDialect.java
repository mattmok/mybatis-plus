/*
 * Copyright (c) 2011-2023, baomidou (jobob@qq.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.baomidou.mybatisplus.extension.plugins.pagination.dialects;

import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.StringPool;
import com.baomidou.mybatisplus.extension.plugins.pagination.DialectModel;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ORACLE 数据库分页语句组装实现
 * 通用分页版本
 *
 * @author hubin
 * @since 2016-01-23
 */
@Slf4j
public class OracleDialect implements IDialect {

    @Override
    public DialectModel buildPaginationSql(String originalSql, long offset, long limit) {
        String columns = getColumnsByOriginalSql(originalSql);
        limit = (offset >= 1) ? (offset + limit) : limit;
        String sql = "SELECT " + columns +" FROM ( SELECT TMP.*, ROWNUM ROW_ID FROM ( " +
            originalSql + " ) TMP WHERE ROWNUM <=" + FIRST_MARK + ") WHERE ROW_ID > " + SECOND_MARK;
        return new DialectModel(sql, limit, offset).setConsumerChain();
    }

    /**
     * 从原始SQL中解析出所有列名
     *
     * @param originalSql
     * @return
     */
    private String getColumnsByOriginalSql(String originalSql) {
        String columnSql = StringPool.ASTERISK;
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            final Statement parse = parserManager.parse(new StringReader(originalSql));
            final Select select = (Select) parse;
            PlainSelect body = (PlainSelect) select.getSelectBody();
            List<String> aliasList = getAliasList(body);
            if (CollectionUtils.isNotEmpty(aliasList)) {
                columnSql = String.join(StringPool.COMMA, aliasList);
            }
        } catch (Exception ignore) {
            log.debug("parsing columns error, original: {}", originalSql);
        }
        return columnSql;
    }

    private List<String> getAliasList(PlainSelect body) {
        final List<SelectItem> selectItems = body.getSelectItems();
        List<String> aliasList = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            final SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
            final Alias alias = expressionItem.getAlias();
            final Expression expression = expressionItem.getExpression();
            if (alias != null) {
                aliasList.add(alias.getName());
            } else if (expression instanceof Column) {
                Column column = (Column) expression;
                aliasList.add(column.getColumnName());
            } else if (expression instanceof Function) {
                final Function function = (Function) expression;
                aliasList.add(function.getName());
            }
        }
        return aliasList;
    }
}
