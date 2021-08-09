/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014-2017 abel533@gmail.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package tk.mybatis.mapper.provider.base;

import org.apache.ibatis.mapping.MappedStatement;
import tk.mybatis.mapper.MapperException;
import tk.mybatis.mapper.entity.EntityColumn;
import tk.mybatis.mapper.mapperhelper.*;

import java.util.Set;

/**
 * BaseInsertProvider实现类，基础方法实现类
 *
 * @author liuzh
 */
public class BaseInsertProvider extends MapperTemplate {
  public BaseInsertProvider(Class<?> mapperClass, MapperHelper mapperHelper) {
    super(mapperClass, mapperHelper);
  }

  /**
   * INSERT INTO tb_datasource_base
   * <trim prefix="(" suffix=")" suffixOverrides=",">
   * user_id,project_id,ds_name,ds_type,ds_link,ds_config_id,ds_desc,row_state,create_time,update_time,"status",status_update_time,
   * </trim>
   * <trim prefix="VALUES(" suffix=")" suffixOverrides=",">
   * <if test="userId != null">#{userId},</if>
   * <if test="userId == null">#{userId},</if>
   * <if test="projectId != null">#{projectId},</if>
   * <if test="projectId == null">#{projectId},</if>
   * <if test="dsName != null">#{dsName},</if>
   * <if test="dsName == null">#{dsName},</if>
   * <if test="dsType != null">#{dsType},</if>
   * <if test="dsType == null">#{dsType},</if>
   * <if test="dsLink != null">#{dsLink},</if>
   * <if test="dsLink == null">#{dsLink},</if>
   * <if test="dsConfigId != null">#{dsConfigId},</if>
   * <if test="dsConfigId == null">#{dsConfigId},</if>
   * <if test="dsDesc != null">#{dsDesc},</if>
   * <if test="dsDesc == null">#{dsDesc},</if>
   * <if test="rowState != null">#{rowState},</if>
   * <if test="rowState == null">#{rowState},</if>
   * <if test="createTime != null">#{createTime},</if>
   * <if test="createTime == null">#{createTime},</if>
   * <if test="updateTime != null">#{updateTime},</if>
   * <if test="updateTime == null">#{updateTime},</if>
   * <if test="status != null">#{status},</if>
   * <if test="status == null">#{status},</if>
   * <if test="statusUpdateTime != null">#{statusUpdateTime},</if>
   * <if test="statusUpdateTime == null">#{statusUpdateTime},</if>
   * </trim>
   */
  public String insert(MappedStatement ms) {
    Class<?> entityClass = getEntityClass(ms);
    StringBuilder sql = new StringBuilder();

    // 获取全部列
    Set<EntityColumn> columnList = EntityHelper.getColumns(entityClass);

    EntityColumn logicDeleteColumn = SqlHelper.getLogicDeleteColumn(entityClass);

    processKey(sql, entityClass, ms, columnList);

    boolean insertWithId = getConfig().isInsertWithId();

    sql.append(SqlHelper.insertIntoTable(entityClass, tableName(entityClass)));
    sql.append(SqlHelper.insertColumns(entityClass, !insertWithId, false, false));

    sql.append("<trim prefix=\"VALUES(\" suffix=\")\" suffixOverrides=\",\">");

    for (EntityColumn column : columnList) {
      if (!column.isInsertable()) {
        continue;
      }

      if (logicDeleteColumn != null && logicDeleteColumn == column) {
        sql.append(SqlHelper.getLogicDeletedValue(column, false)).append(",");
        continue;
      }

      // 优先使用传入的属性值,当原属性property!=null时，用原属性

      if (column.isIdentity()) {
        if (insertWithId) {
          // 自增的情况下,如果默认有值,就会备份到property_cache中,所以这里需要先判断备份的值是否存在
          sql.append(
              SqlHelper.getIfCacheNotNull(column, column.getColumnHolder(null, "_cache", ",")));

          // 当属性为null时，如果存在主键策略，会自动获取值，如果不存在，则使用null
          sql.append(SqlHelper.getIfCacheIsNull(column, column.getColumnHolder() + ","));
        }
      } else {
        // 其他情况值仍然存在原property中
        sql.append(
            SqlHelper.getIfNotNull(column, column.getColumnHolder(null, null, ","), isNotEmpty()));

        // 当null的时候，如果不指定jdbcType，oracle可能会报异常，指定VARCHAR不影响其他
        sql.append(
            SqlHelper.getIfIsNull(column, column.getColumnHolder(null, null, ","), isNotEmpty()));
      }
    }
    sql.append("</trim>");
    return sql.toString();
  }

  /**
   * INSERT INTO tb_datasource_base
   * <trim prefix="(" suffix=")" suffixOverrides=",">
   * <if test="userId != null">user_id,</if>
   * <if test="projectId != null">project_id,</if>
   * <if test="dsName != null">ds_name,</if>
   * <if test="dsType != null">ds_type,</if>
   * <if test="dsLink != null">ds_link,</if>
   * <if test="dsConfigId != null">ds_config_id,</if>
   * <if test="dsDesc != null">ds_desc,</if>
   * <if test="rowState != null">row_state,</if>
   * <if test="createTime != null">create_time,</if>
   * <if test="updateTime != null">update_time,</if>
   * <if test="status != null">"status",</if>
   * <if test="statusUpdateTime != null">status_update_time,</if>
   * </trim>
   * <trim prefix="VALUES(" suffix=")" suffixOverrides=",">
   * <if test="userId != null">#{userId},</if>
   * <if test="projectId != null">#{projectId},</if>
   * <if test="dsName != null">#{dsName},</if>
   * <if test="dsType != null">#{dsType},</if>
   * <if test="dsLink != null">#{dsLink},</if>
   * <if test="dsConfigId != null">#{dsConfigId},</if>
   * <if test="dsDesc != null">#{dsDesc},</if>
   * <if test="rowState != null">#{rowState},</if>
   * <if test="createTime != null">#{createTime},</if>
   * <if test="updateTime != null">#{updateTime},</if>
   * <if test="status != null">#{status},</if>
   * <if test="statusUpdateTime != null">#{statusUpdateTime},</if>
   * </trim>
   */
  public String insertSelective(MappedStatement ms) {
    Class<?> entityClass = getEntityClass(ms);
    StringBuilder sql = new StringBuilder();

    // 获取全部列
    Set<EntityColumn> columnList = EntityHelper.getColumns(entityClass);
    EntityColumn logicDeleteColumn = SqlHelper.getLogicDeleteColumn(entityClass);

    processKey(sql, entityClass, ms, columnList);

    sql.append(SqlHelper.insertIntoTable(entityClass, tableName(entityClass)));
    sql.append("<trim prefix=\"(\" suffix=\")\" suffixOverrides=\",\">");

    boolean insertWithId = getConfig().isInsertWithId();

    for (EntityColumn column : columnList) {
      if (!column.isInsertable()) {
        continue;
      }

      if (column.isIdentity()) {
        if (insertWithId) {
          sql.append(column.getColumn()).append(",");
        }
      } else {
        if (logicDeleteColumn != null && logicDeleteColumn == column) {
          sql.append(column.getColumn()).append(",");
          continue;
        }

        sql.append(SqlHelper.getIfNotNull(column, column.getColumn() + ",", isNotEmpty()));
      }
    }
    sql.append("</trim>");

    sql.append("<trim prefix=\"VALUES(\" suffix=\")\" suffixOverrides=\",\">");
    for (EntityColumn column : columnList) {
      if (!column.isInsertable()) {
        continue;
      }

      if (logicDeleteColumn != null && logicDeleteColumn == column) {
        sql.append(SqlHelper.getLogicDeletedValue(column, false)).append(",");
        continue;
      }

      // 优先使用传入的属性值,当原属性property!=null时，用原属性

      if (column.isIdentity()) {
        if (insertWithId) {
          // 自增的情况下,如果默认有值,就会备份到property_cache中,所以这里需要先判断备份的值是否存在
          sql.append(
              SqlHelper.getIfCacheNotNull(column, column.getColumnHolder(null, "_cache", ",")));

          // 当属性为null时，如果存在主键策略，会自动获取值，如果不存在，则使用null序列的情况
          sql.append(SqlHelper.getIfCacheIsNull(column, column.getColumnHolder() + ","));
        }

      } else {
        // 其他情况值仍然存在原property中
        sql.append(
            SqlHelper.getIfNotNull(column, column.getColumnHolder(null, null, ","), isNotEmpty()));
      }
    }
    sql.append("</trim>");

    return sql.toString();
  }

  private void processKey(
      StringBuilder sql, Class<?> entityClass, MappedStatement ms, Set<EntityColumn> columnList) {
    // Identity列只能有一个
    boolean hasIdentityKey = false;
    // 先处理cache或bind节点
    for (EntityColumn column : columnList) {
      if (column.isIdentity()) {
        // 这种情况下,如果原先的字段有值,需要先缓存起来,否则就一定会使用自动增长
        // 这是一个bind节点
        sql.append(SqlHelper.getBindCache(column));
        // 如果是Identity列，就需要插入selectKey
        // 如果已经存在Identity列，抛出异常
        if (hasIdentityKey) {
          // jdbc类型只需要添加一次
          if (column.getGenerator() != null && "JDBC".equals(column.getGenerator())) {
            continue;
          }
          throw new MapperException(
              ms.getId() + "对应的实体类" + entityClass.getCanonicalName() + "中包含多个MySql的自动增长列,最多只能有一个!");
        }
        // 插入selectKey
        SelectKeyHelper.newSelectKeyMappedStatement(
            ms, column, entityClass, isBEFORE(), getIDENTITY(column));
        hasIdentityKey = true;
      } else if (column.getGenIdClass() != null) {
        sql.append("<bind name=\"")
            .append(column.getColumn())
            .append("GenIdBind\" value=\"@tk.mybatis.mapper.genid.GenIdUtil@genId(");
        sql.append("_parameter").append(", '").append(column.getProperty()).append("'");
        sql.append(", @").append(column.getGenIdClass().getCanonicalName()).append("@class");
        sql.append(", '").append(tableName(entityClass)).append("'");
        sql.append(", '").append(column.getColumn()).append("')");
        sql.append("\"/>");
      }
    }
  }
}
