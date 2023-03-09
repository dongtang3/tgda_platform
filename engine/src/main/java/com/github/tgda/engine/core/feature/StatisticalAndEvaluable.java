package com.github.tgda.engine.core.feature;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.RelationKindMatchLogic;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.payload.AttributeValueDistributionInfo;
import com.github.tgda.engine.core.payload.GroupNumericalAttributesStatisticResult;
import com.github.tgda.engine.core.payload.TypeEntityAttributeRuntimeStatistics;
import com.github.tgda.engine.core.payload.NumericalAttributeStatisticCondition;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.Entity;

import java.util.List;
import java.util.Map;

public interface StatisticalAndEvaluable {

    public enum StatisticFunction {
        COUNT,AVG,MAX,MIN,STDEV,SUM
    }

    public enum EvaluateFunction {}

    /**
     * 统计数值类属性信息
     *
     * @param queryParameters QueryParameters 待统计数据查询条件
     * @param statisticCondition List<NumericalAttributeStatisticCondition> 数值类属性数据统计条件列表
     *
     * @return 统计结果键值对
     */
    public Map<String,Number> statisticNumericalAttributes(QueryParameters queryParameters, List<NumericalAttributeStatisticCondition> statisticCondition)  throws EngineServiceEntityExploreException;

    /**
     * 按属性值分组统计数值类属性信息
     *
     * @param groupByAttribute String 待分组属性名称
     * @param queryParameters QueryParameters 待统计数据查询条件
     * @param statisticConditions  List<NumericalAttributeStatisticCondition>  数值类属性数据统计条件列表
     *
     * @return 带分组信息的统计结果键值对列表
     */
    public List<GroupNumericalAttributesStatisticResult> statisticNumericalAttributesByGroup(String groupByAttribute, QueryParameters queryParameters, List<NumericalAttributeStatisticCondition> statisticConditions) throws EngineServiceEntityExploreException;

    /**
     * 统计特定实体对象按照指定关联规律与分类的关联分组
     *
     * @param queryParameters QueryParameters 待统计数据查询条件
     * @param relationKindName String 与分类关联的关系类型名称
     * @param direction Direction 与分类关联的关系方向
     *
     * @return 统计结果键值对，Key 为分类名称， Value 为与其相关的概念实体列表
     */
    public Map<String,List<Entity>> statisticRelatedClassifications(QueryParameters queryParameters, String relationKindName, Direction direction) throws EngineServiceEntityExploreException;

    /**
     * 统计类型已有数据实体中存在的所有 Attribute 属性信息
     *
     * @param sampleCount long 执行统计时抽样的实体数据数量
     *
     * @return 类型中抽样选取的数据实体中已经存在的所有属性的统计信息列表
     */
    public List<TypeEntityAttributeRuntimeStatistics> statisticEntityAttributesDistribution(long sampleCount);

    /**
     * 统计特定实体对象按照指定关联规律的 Degree 值
     *
     * @param queryParameters AttributesParameters 待统计数据查询条件
     * @param relationKindMatchLogics List<RelationKindMatchLogic> 路径上允许的关系类型名称与关系方向组合
     * @param defaultDirectionForNoneRelationKindMatch Direction 未输入目标关系类型名称与关系方向组合时使用的全局关系方向
     *
     * @return 统计结果Map， key为实体对象的唯一值ID，value 为 degree 数值
     */
    public Map<String,Long> statisticEntityRelationDegree(AttributesParameters queryParameters, List<RelationKindMatchLogic> relationKindMatchLogics, Direction defaultDirectionForNoneRelationKindMatch) throws EngineServiceEntityExploreException;

    /**
     * 统计特定实体对象中指定数值类属性的数据分布信息
     *
     * @param queryParameters AttributesParameters 待统计数据查询条件
     * @param attributeName String 待统计数值类属性名称
     *
     * @return 属性值分布信息对象
     */
    public AttributeValueDistributionInfo statisticAttributeValueDistribution(AttributesParameters queryParameters, String attributeName) throws EngineServiceEntityExploreException;
}
