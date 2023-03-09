package com.github.tgda.engine.core.term;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.ClassificationAttachable;
import com.github.tgda.engine.core.feature.MetaAttributeFeatureSupportable;
import com.github.tgda.engine.core.feature.MetaConfigItemFeatureSupportable;
import com.github.tgda.engine.core.feature.StatisticalAndEvaluable;
import com.github.tgda.engine.core.payload.*;
import com.github.tgda.engine.core.structure.InheritanceTree;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Type extends MetaConfigItemFeatureSupportable, MetaAttributeFeatureSupportable, ClassificationAttachable, StatisticalAndEvaluable {
    //东风夜放花千树，更吹落，星如雨
    /**
     * 获取当前概念类型对象名称
     *
     * @return 概念类型对象名称
     */
    public String getConceptionKindName();

    /**
     * 获取当前概念类型对象描述
     *
     * @return 概念类型对象描述
     */
    public String getConceptionKindDesc();

    /**
     * 更新当前概念类型对象描述
     *
     * @param kindDesc String 新的概念类型描述
     *
     * @return 如操作成功，返回结果为 true
     */
    public boolean updateTypeDesc(String kindDesc);

    /**
     * 计算当前概念类型的所有概念实体数量
     *
     * @return 概念实体数量
     */
    public Long countConceptionEntities() throws EngineServiceRuntimeException;

    /**
     * 计算当前概念类型（包含所有后代概念类型）的所有概念实体数量，该方法在 NEO4J 实现类型下无效
     *
     * @return 概念实体数量
     */
    public Long countConceptionEntitiesWithOffspring() throws EngineFunctionNotSupportedException;

    /**
     * 获取当前概念类型的所有子概念类型对象，该方法在 NEO4J 实现类型下无效
     *
     * @return 概念类型对象列表
     */
    public List<Type> getChildConceptionKinds() throws EngineFunctionNotSupportedException;

    /**
     * 获取当前概念类型的父概念类型对象，该方法在 NEO4J 实现类型下无效
     *
     * @return 概念类型对象
     */
    public Type getParentConceptionKind() throws EngineFunctionNotSupportedException;

    /**
     * 获取当前概念类型的所有后代概念类型对象，该方法在 NEO4J 实现类型下无效
     *
     * @return 概念类型对象继承树
     */
    public InheritanceTree<Type> getOffspringConceptionKinds() throws EngineFunctionNotSupportedException;

    /**
     * 创建一个属于当前概念类型的概念实体对象
     *
     * @param conceptionEntityValue EntityValue 概念实体属性值
     * @param addPerDefinedRelation boolean 是否根据预定义的关联逻辑建立关系链接
     *
     * @return 概念实体对象
     */
    public Entity newEntity(EntityValue conceptionEntityValue, boolean addPerDefinedRelation);

    /**
     * 创建一个属于当前概念类型的概念实体对象
     *
     * @param conceptionEntityValue EntityValue 概念实体属性值
     * @param relationshipAttachList List<RelationAttachKind> 建立链接所需的关系附着规则类型列表
     * @param entityRelateRole EntityRelateRole 概念实体在关系中的角色
     *
     * @return 概念实体对象
     */
    public Entity newEntity(EntityValue conceptionEntityValue, List<RelationshipAttach> relationshipAttachList, RelationshipAttach.EntityRelateRole entityRelateRole);

    /**
     * 创建多个属于当前概念类型的概念实体对象
     * @param conceptionEntityValues List<EntityValue> 概念实体属性值列表
     * @param addPerDefinedRelation boolean 是否根据预定义的关联逻辑建立关系链接
     *
     * @return 实体对象操作返回结果
     */
    public EntitiesOperationResult newEntities(List<EntityValue> conceptionEntityValues, boolean addPerDefinedRelation);

    /**
     * 创建多个属于当前概念类型的概念实体对象
     *
     * @param conceptionEntityValues List<EntityValue> 概念实体属性值列表
     * @param relationshipAttachList List<RelationAttachKind> 建立链接所需的关系附着规则类型列表
     * @param entityRelateRole EntityRelateRole 概念实体在关系中的角色
     * @return 实体对象操作返回结果
     */
    public EntitiesOperationResult newEntities(List<EntityValue> conceptionEntityValues, List<RelationshipAttach> relationshipAttachList, RelationshipAttach.EntityRelateRole entityRelateRole);

    /**
     * 更新一个当前概念类型的概念实体对象的属性信息
     *
     * @param conceptionEntityValueForUpdate EntityValue 需要更新的概念实体信息
     *
     * @return 更新后的概念实体对象
     */
    public Entity updateEntity(EntityValue conceptionEntityValueForUpdate) throws EngineServiceRuntimeException;

    /**
     * 更新多个当前概念类型的概念实体对象的属性信息
     *
     * @param entityValues List<EntityValue> 需要更新的概念实体信息
     *
     * @return 实体对象操作返回结果
     */
    public EntitiesOperationResult updateEntities(List<EntityValue> entityValues);

    /**
     * 删除一个当前概念类型的概念实体对象
     *
     * @param conceptionEntityUID String 需要删除的概念实体的唯一ID
     *
     * @return 如操作成功，返回结果为 true
     */
    public boolean deleteEntity(String conceptionEntityUID) throws EngineServiceRuntimeException;

    /**
     * 更新多个当前概念类型的概念实体对象
     *
     * @param conceptionEntityUIDs List<String> 需要删除的概念实体的唯一ID列表
     *
     * @return 实体对象操作返回结果
     */
    public EntitiesOperationResult deleteEntities(List<String> conceptionEntityUIDs) throws EngineServiceRuntimeException;

    /**
     * 删除当前概念类型的所有概念实体
     *
     * @return 实体对象操作返回结果
     */
    public EntitiesOperationResult purgeAllEntities() throws EngineServiceRuntimeException;

    /**
     * 计算符合过滤条件的当前概念类型的概念实体对象数量
     *
     * @param attributesParameters AttributesParameters 查询过滤条件
     * @param isDistinctMode boolean 是否不允许重复数据
     *
     * @return 概念实体数量
     */
    public Long countEntities(AttributesParameters attributesParameters, boolean isDistinctMode) throws EngineServiceEntityExploreException, EngineServiceRuntimeException;

    /**
     * 查询符合过滤条件的当前概念类型的概念实体对象
     *
     * @param queryParameters QueryParameters 查询过滤条件
     *
     * @return 概念实体查询结果集
     */
    public EntitiesRetrieveResult getEntities(QueryParameters queryParameters) throws EngineServiceEntityExploreException;

    /**
     * 根据唯一ID获取当前概念类型的概念实体对象
     *
     * @param conceptionEntityUID String 需要获取的概念实体唯一ID
     *
     * @return 概念实体对象
     */
    public Entity getEntityByUID(String conceptionEntityUID);

    /**
     * 查询符合过滤条件的当前概念类型的概念实体对象,并根据输入的 SINGLE_VALUE 数据存储结构的属性视图类型列表，合并其中包含的属性类型返回相应的属性值
     *
     * @param attributesViewKindNames List<String> 属性视图类型列表
     * @param exploreParameters QueryParameters 查询过滤条件
     *
     * @return 概念实体属性查询结果集
     */
    public EntitiesAttributesRetrieveResult getSingleValueEntityAttributesByViewKinds(List<String> attributesViewKindNames, QueryParameters exploreParameters) throws EngineServiceEntityExploreException;

    /**
     * 查询符合过滤条件的当前概念类型的概念实体对象,并根据输入的属性类型返回相应的属性值
     *
     * @param attributeNames List<String> 属性类型列表
     * @param exploreParameters QueryParameters 查询过滤条件
     *
     * @return 概念实体属性查询结果集
     */
    public EntitiesAttributesRetrieveResult getSingleValueEntityAttributesByAttributeNames(List<String> attributeNames, QueryParameters exploreParameters) throws EngineServiceEntityExploreException;

    /**
     * 为当前概念类型附加属性视图类型
     *
     * @param attributesViewKindUID String 需要附加的属性视图类型唯一ID
     *
     * @return 如操作成功，返回结果为 true
     */
    public boolean attachAttributesViewKind(String attributesViewKindUID) throws EngineServiceRuntimeException;

    /**
     * 获取当前概念类型附加的全部属性视图类型
     *
     * @return 属性视图类型对象列表
     */
    public List<AttributesView> getContainsAttributesViewKinds();

    /**
     * 获取当前概念类型附加的全部符合名称查询条件的属性视图类型
     *
     * @param attributesViewKindName String 需要返回的属性视图类型名称，本查询的数值匹配规则为 Equal 匹配
     *
     * @return 属性视图类型对象列表
     */
    public List<AttributesView> getContainsAttributesViewKinds(String attributesViewKindName);

    /**
     * 从当前概念类型上移除已经附加的属性视图类型
     *
     * @param attributesViewKindUID String 需要移除的属性视图类型唯一ID
     *
     * @return 如操作成功，返回结果为 true
     */
    public boolean detachAttributesViewKind(String attributesViewKindUID) throws EngineServiceRuntimeException;

    /**
     * 获取当前概念类型包含的全部 SINGLE_VALUE 数据存储结构的属性视图类型中包含的属性类型
     *
     * @return 属性类型对象列表
     */
    public List<Attribute> getContainsSingleValueAttributeKinds();

    /**
     * 获取当前概念类型包含的全部符合名称查询条件的 SINGLE_VALUE 数据存储结构的属性视图类型中包含的属性类型
     *
     * @param attributeKindName String 需要返回的属性类型名称，本查询的数值匹配规则为 Equal 匹配
     *
     * @return 属性类型对象列表
     */
    public List<Attribute> getContainsSingleValueAttributeKinds(String attributeKindName);

    /**
     * 查询符合过滤条件的与当前概念类型的实体对象直接关联的其他实体对象
     *
     * @param startEntityUIDS List<String> 当前概念类型实体对象 UID 列表，只有与该列表中UID定义的实体对象关联的数据才会计入查询过滤结果，如传入 null 则忽略特定实体关联，从类型上执行全局查询
     * @param relationKind String 关联的关系类型名称
     * @param direction Direction 关联方向
     * @param aimConceptionKind List<String> 查询目标概念类型名称，如传入 null 则忽略类型
     * @param queryParameters QueryParameters 查询返回的概念实体过滤参数
     *
     * @return 概念实体查询结果集
     */
    public EntitiesRetrieveResult getKindDirectRelatedEntities(List<String> startEntityUIDS, String relationKind, Direction
            direction, String aimConceptionKind, QueryParameters queryParameters) throws EngineServiceEntityExploreException;

    /**
     * 查询符合过滤条件的与当前概念类型的实体对象直接关联的其他实体对象,并根据输入的属性类型返回相应的属性值
     *
     * @param startEntityUIDS List<String> 当前概念类型实体对象 UID 列表，只有与该列表中UID定义的实体对象关联的数据才会计入查询过滤结果，如传入 null 则忽略特定实体关联，从类型上执行全局查询
     * @param attributeNames List<String> 返回属性类型列表
     * @param relationKind String 关联的关系类型名称
     * @param direction Direction 关联方向
     * @param aimConceptionKind List<String> 查询目标概念类型名称，如传入 null 则忽略类型
     * @param queryParameters QueryParameters 查询返回的概念实体过滤参数
     *
     * @return 概念实体属性查询结果集
     */
    public EntitiesAttributesRetrieveResult getAttributesOfKindDirectRelatedEntities(List<String> startEntityUIDS, List<String> attributeNames, String relationKind, Direction
            direction, String aimConceptionKind, QueryParameters queryParameters) throws EngineServiceEntityExploreException;

    /**
     * 查询与符合过滤条件的目标概念类型实体直接关联的实体对象
     *
     * @param relationKind String 关联的关系类型名称
     * @param direction Direction 关联方向
     * @param aimConceptionKind List<String> 查询目标概念类型名称，如传入 null 则忽略类型
     * @param queryParameters QueryParameters 查询的目标关联概念实体过滤参数
     *
     * @return 概念实体查询结果集
     */
    public EntitiesRetrieveResult getEntitiesByDirectRelations(String relationKind, Direction
            direction, String aimConceptionKind, QueryParameters queryParameters) throws EngineServiceEntityExploreException;
    /**
     * 根据采样率获取部分概念实体数据并统计其中包含的属性分布统计信息
     *
     * @param sampleRatio double 采样率，介于0到1之间的小数，代表当前概念类型中需要采样数据的百分比
     *
     * @return 属性分布统计的结果信息
     */
    public Set<AttributeDistributionInfo> getKindAttributesDistributionStatistics(double sampleRatio) throws EngineServiceRuntimeException;

    /**
     * 根据采样率获取部分概念实体数据并统计其中包含的属性与关联关系数据的分布统计信息
     *
     * @param sampleRatio double 采样率，介于0到1之间的小数，代表当前概念类型中需要采样数据的百分比
     *
     * @return 属性与关联关系数据分布统计的结果信息
     */
    public Set<TypeDataDistributionInfo> getKindDataDistributionStatistics(double sampleRatio) throws EngineServiceRuntimeException;

    /**
     * 统计当前概念类型实体与其他概念类型实体之间的实时关联关系信息
     *
     * @return 当前概念类型与其他概念类型之间的关联关系信息集合
     */
    public Set<TypeCorrelationInfo> getKindRelationDistributionStatistics();

    /**
     * 随机获取若干当前概念类型下的概念实体
     *
     * @param entitiesCount int 需要获取的概念实体数量
     *
     * @return 概念实体集合
     */
    public Set<Entity>  getRandomEntities(int entitiesCount) throws EngineServiceEntityExploreException;

    /**
     * 随机获取若干符合过滤条件的当前概念类型的概念实体
     *
     * @param attributesParameters AttributesParameters 查询过滤条件
     * @param isDistinctMode boolean 是否不允许重复数据
     * @param entitiesCount int 需要获取的概念实体数量
     *
     * @return 概念实体集合
     */
    public Set<Entity> getRandomEntities(AttributesParameters attributesParameters, boolean isDistinctMode, int entitiesCount) throws EngineServiceEntityExploreException, EngineServiceRuntimeException;

    /**
     * 为当前概念类型的所有概念实体添加指定的属性，如属性已经存在，则用新的值覆盖原有属性
     *
     * @param attributes Map<String, Object> 需要添加的所有属性
     *
     * @return 操作成功的数据总量
     */
    public long setKindScopeAttributes(Map<String, Object> attributes);
}
