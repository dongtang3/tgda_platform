package com.github.tgda.engine.core.util;

public interface Constant {

    public final String RealmInnerTypePerFix="TGDA_";
    public final String TypeClass="TGDA_Type";
    public final String AttributesViewKindClass="TGDA_AttributesViewKind";
    public final String AttributeClass ="TGDA_AttributeKind";
    public final String RelationKindClass="TGDA_RelationKind";
    public final String MetaConfigItemsStorageClass="TGDA_MetaConfigItemsStorage";
    public final String ClassificationClass="TGDA_Classification";
    public final String Type_AttributesViewRelationClass="TGDA_ConceptionContainsViewKindIs";
    public final String Kind_MetaConfigItemsStorageRelationClass ="TGDA_MetaConfigItemsStorageIs";
    public final String AttributesViewKind_AttributeRelationClass="TGDA_ViewContainsAttributeKindIs";
    public final String Classification_ClassificationRelationClass="TGDA_ParentClassificationIs";
    public final String RelationAttachKindClass="TGDA_RelationAttachKind";
    public final String RelationAttachLinkLogicClass="TGDA_RelationAttachLinkLogic";
    public final String RelationAttachKind_RelationAttachLinkLogicRelationClass="TGDA_AttachKindContainsAttachLinkLogicIs";

    public final String TimeFlowClass="TGDA_TimeFlow";
    public final String _defaultTimeFlowName = "DefaultTimeFlow";
    public final String TimeScaleEntityClass="TGDA_TimeScaleEntity";
    public final String TimeScaleYearEntityClass="TGDA_TS_Year";
    public final String TimeScaleMonthEntityClass="TGDA_TS_Month";
    public final String TimeScaleDayEntityClass="TGDA_TS_Day";
    public final String TimeScaleHourEntityClass="TGDA_TS_Hour";
    public final String TimeScaleMinuteEntityClass="TGDA_TS_Minute";
    public final String TimeScale_ContainsRelationClass="TGDA_TS_Contains";
    public final String TimeScale_NextIsRelationClass="TGDA_TS_NextIs";
    public final String TimeScale_FirstChildIsRelationClass="TGDA_TS_FirstChildIs";
    public final String TimeScale_LastChildIsRelationClass="TGDA_TS_LastChildIs";
    public final String TimeScaleEventClass="TGDA_TimeScaleEvent";
    public final String TimeScale_TimeReferToRelationClass="TGDA_TS_TimeReferTo";
    public final String TimeScale_AttachToRelationClass="TGDA_AttachToTimeScale";

    public final String GeospatialClass="TGDA_Geospatial";
    public final String _defaultGeospatialName = "DefaultGeospatial";
    public final String GeospatialScaleEntityClass="TGDA_GeospatialScaleEntity";
    public final String GeospatialScaleContinentEntityClass="TGDA_GS_Continent";
    public final String GeospatialScaleCountryRegionEntityClass="TGDA_GS_CountryRegion";
    public final String GeospatialScaleProvinceEntityClass="TGDA_GS_Province";
    public final String GeospatialScalePrefectureEntityClass="TGDA_GS_Prefecture";
    public final String GeospatialScaleCountyEntityClass="TGDA_GS_County";
    public final String GeospatialScaleTownshipEntityClass="TGDA_GS_Township";
    public final String GeospatialScaleVillageEntityClass="TGDA_GS_Village";

    public final String GeospatialScale_SpatialContainsRelationClass="TGDA_GS_SpatialContains"; //空间包含
    public final String GeospatialScale_SpatialIdenticalRelationClass="TGDA_GS_SpatialIdentical";//空间相同
    public final String GeospatialScale_SpatialApproachRelationClass="TGDA_GS_SpatialApproach";//空间相邻
    public final String GeospatialScale_SpatialConnectRelationClass="TGDA_GS_SpatialConnect";//空间相连

    public final String GeospatialScaleEventClass="TGDA_GeospatialScaleEvent";
    public final String GeospatialScale_GeospatialReferToRelationClass="TGDA_GS_GeospatialReferTo";
    public final String GeospatialScale_AttachToRelationClass="TGDA_AttachToGeospatialScale";

    public final String _NameProperty = "name";
    public final String _DescProperty = "description";
    public final String _createDateProperty = "createDate";
    public final String _lastModifyDateProperty = "lastModifyDate";
    public final String _creatorIdProperty = "creatorId";
    public final String _dataOriginProperty = "dataOrigin";

    public final String _viewKindDataForm = "viewKindDataForm";

    public final String _attributeDataType = "attributeDataType";

    public final String _relationAttachSourceKind = "attachSourceKind";
    public final String _relationAttachTargetKind = "attachTargetKind";
    public final String _relationAttachRelationKind = "attachRelationKind";
    public final String _relationAttachRepeatableRelationKind = "attachAllowRepeatRelation";

    public final String _attachLinkLogicType = "linkLogicType";
    public final String _attachLinkLogicCondition = "linkLogicCondition";
    public final String _attachLinkLogicSourceAttribute = "linkLogicSourceAttribute";
    public final String _attachLinkLogicTargetAttribute = "linkLogicTargetAttribute";

    public final String _TimeScaleEventComment="TGDA_TimeScaleEventComment";
    public final String _TimeScaleEventReferTime="TGDA_TimeScaleEventReferTime";
    public final String _TimeScaleEventScaleGrade="TGDA_TimeScaleEventScaleGrade";
    public final String _TimeScaleEventTimeFlow="TGDA_TimeScaleEventTimeFlow";

    public final String _GeospatialScaleEventReferLocation="TGDA_GeospatialScaleEventReferLocation";
    public final String _GeospatialScaleEventComment="TGDA_GeospatialScaleEventComment";
    public final String _GeospatialScaleEventScaleGrade="TGDA_GeospatialScaleEventScaleGrade";
    public final String _GeospatialScaleEventGeospatial="TGDA_GeospatialScaleEventGeospatial";

    public final String GeospatialProperty = "TGDA_Geospatial";
    public final String GeospatialCodeProperty = "TGDA_GeospatialCode";
    public final String GeospatialChineseNameProperty = "TGDA_GeospatialChineseName";
    public final String GeospatialEnglishNameProperty = "TGDA_GeospatialEnglishName";
    public final String GeospatialScaleGradeProperty = "TGDA_GeospatialScaleGrade";

    public final String _GeospatialGeometryType="TGDA_GS_GeometryType";
    public final String _GeospatialGlobalCRSAID="TGDA_GS_GlobalCRSAID";
    public final String _GeospatialCountryCRSAID="TGDA_GS_CountryCRSAID";
    public final String _GeospatialLocalCRSAID="TGDA_GS_LocalCRSAID";
    public final String _GeospatialGLGeometryContent="TGDA_GS_GLGeometryContent";
    public final String _GeospatialCLGeometryContent="TGDA_GS_CLGeometryContent";
    public final String _GeospatialLLGeometryContent="TGDA_GS_LLGeometryContent";
    public final String _GeospatialGLGeometryPOI="TGDA_GS_GLGeometryPOI";
    public final String _GeospatialCLGeometryPOI="TGDA_GS_CLGeometryPOI";
    public final String _GeospatialLLGeometryPOI="TGDA_GS_LLGeometryPOI";
    public final String _GeospatialGLGeometryBorder="TGDA_GS_GLGeometryBorder";
    public final String _GeospatialCLGeometryBorder="TGDA_GS_CLGeometryBorder";
    public final String _GeospatialLLGeometryBorder="TGDA_GS_LLGeometryBorder";
}