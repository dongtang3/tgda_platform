package com.github.tgda.example.generator;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.Type;
import com.google.common.collect.Lists;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.NullValueFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SongPlaylists_Realm_Generator {

    private static final String SongConceptionType = "Song";
    private static final String SongId = "songId";
    private static final String SongTitle = "songTitle";
    private static final String SongArtist = "songArtist";

    private static final String MusicTagConceptionType = "MusicTag";
    private static final String TagId = "tagId";
    private static final String TagName = "tagName";

    private static final String PlaylistConceptionType = "Playlist";
    private static final String PlaylistId = "playlistId";
    private static final String PlaylistContent = "playlistContent";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {

        Engine coreRealm = EngineFactory.getDefaultEngine();

        //Part 1
        Type _MusicTagType = coreRealm.getType(MusicTagConceptionType);
        if(_MusicTagType != null){
            coreRealm.removeType(MusicTagConceptionType,true);
        }
        _MusicTagType = coreRealm.getType(MusicTagConceptionType);
        if(_MusicTagType == null){
            _MusicTagType = coreRealm.createType(MusicTagConceptionType,"音乐分类");
        }

        List<EntityValue> musicTagEntityValueList = new ArrayList<>();
        File file = new File("realmExampleData/song_playlists/tag_hash.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {

                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split(", ");
                    String musicTagId = dataItems[0].trim();
                    String musicTagName = dataItems[1].trim();
                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(TagId,musicTagId);
                    newEntityValueMap.put(TagName,musicTagName);
                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    musicTagEntityValueList.add(entityValue);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        _MusicTagType.newEntities(musicTagEntityValueList,false);

        Type _SongType = coreRealm.getType(SongConceptionType);
        if(_SongType != null){
            coreRealm.removeType(SongConceptionType,true);
        }
        _SongType = coreRealm.getType(SongConceptionType);
        if(_SongType == null){
            _SongType = coreRealm.createType(SongConceptionType,"歌曲");
        }

        List<EntityValue> songEntityValueList = new ArrayList<>();
        File file2 = new File("realmExampleData/song_playlists/song_hash.txt");
        BufferedReader reader2 = null;
        try {
            reader2 = new BufferedReader(new FileReader(file2));
            String tempStr;
            while ((tempStr = reader2.readLine()) != null) {
                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split("\t");
                    String songId = dataItems[0].trim();
                    String songTitle = dataItems[1].trim();
                    String songArtist = dataItems[2].trim();

                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(SongId,songId);
                    newEntityValueMap.put(SongTitle,songTitle);
                    newEntityValueMap.put(SongArtist,songArtist);
                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    songEntityValueList.add(entityValue);
                }
            }
            reader2.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader2 != null) {
                try {
                    reader2.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        _SongType.newEntities(songEntityValueList,false);

        coreRealm.openGlobalSession();

        Type _MusicTagType1 = coreRealm.getType(MusicTagConceptionType);

        QueryParameters queryParameters1 = new QueryParameters();
        NullValueFilteringItem defaultFilterItem1 = new NullValueFilteringItem(TagId);
        defaultFilterItem1.reverseCondition();
        queryParameters1.setDefaultFilteringItem(defaultFilterItem1);

        EntitiesRetrieveResult _MusicTagResult= _MusicTagType1.getEntities(queryParameters1);
        Map<String,String> idUIDMapping_MusicTag = new HashMap();
        for(Entity currentMusicTagEntity : _MusicTagResult.getConceptionEntities()){
            String uid = currentMusicTagEntity.getEntityUID();
            String idValue = currentMusicTagEntity.getAttribute(TagId).getAttributeValue().toString();
            idUIDMapping_MusicTag.put(idValue,uid);
        }

        Type _SongType1 = coreRealm.getType(SongConceptionType);

        QueryParameters queryParameters2 = new QueryParameters();
        NullValueFilteringItem defaultFilterItem2 = new NullValueFilteringItem(SongId);
        defaultFilterItem2.reverseCondition();
        queryParameters2.setDefaultFilteringItem(defaultFilterItem2);

        List<String> attributeNamesList0 = new ArrayList<>();
        attributeNamesList0.add(SongId);
        EntitiesAttributesRetrieveResult entitiesAttributesRetrieveResult0 = _SongType1.getSingleValueEntityAttributesByAttributeNames(attributeNamesList0,queryParameters2);
        List<EntityValue> entityValueList0 = entitiesAttributesRetrieveResult0.getEntityValues();

        Map<String,String> idUIDMapping_Song = new HashMap();
        for(EntityValue currentSongEntityValue : entityValueList0){
            String uid = currentSongEntityValue.getEntityUID();
            String idValue = currentSongEntityValue.getEntityAttributesValue().get(SongId).toString();
            idUIDMapping_Song.put(idValue,uid);
        }

        File file3 = new File("realmExampleData/song_playlists/tags.txt");
        BufferedReader reader3 = null;
        try {
            reader3 = new BufferedReader(new FileReader(file3));
            String tempStr;
            int keyIndex = 0;
            while ((tempStr = reader3.readLine()) != null) {
                String currentSongId = ""+keyIndex;
                keyIndex ++;
                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    String songEntityUID = idUIDMapping_Song.get(currentSongId);
                    Entity _SongEntity = _SongType1.getEntityByUID(songEntityUID);
                    String[] dataItems =  currentLine.split(" ");
                    for(String currentTagId:dataItems){
                        linkSongToTagItem(_SongEntity,idUIDMapping_Song,idUIDMapping_MusicTag,currentTagId);
                    }
                }
            }
            reader3.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader3 != null) {
                try {
                    reader3.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        coreRealm.closeGlobalSession();

        Type _PlaylistType = coreRealm.getType(PlaylistConceptionType);
        if(_PlaylistType != null){
            coreRealm.removeType(PlaylistConceptionType,true);
        }
        _PlaylistType = coreRealm.getType(PlaylistConceptionType);
        if(_PlaylistType == null){
            _PlaylistType = coreRealm.createType(PlaylistConceptionType,"播放列表");
        }

        List<EntityValue> playlistEntityValueList = Lists.newArrayList();
        File file4 = new File("realmExampleData/song_playlists/test.txt");
        BufferedReader reader4 = null;
        try {
            reader4 = new BufferedReader(new FileReader(file4));
            String tempStr;
            int keyIndex = 0;
            while ((tempStr = reader4.readLine()) != null) {
                String currentPlayListId = ""+keyIndex;
                keyIndex ++;
                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(PlaylistId,currentPlayListId);
                    newEntityValueMap.put(PlaylistContent,currentLine);
                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    playlistEntityValueList.add(entityValue);
                }
            }
            reader4.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader4 != null) {
                try {
                    reader4.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        class InsertRecordThread implements Runnable{
            private List<EntityValue> conceptionEntityValueList;
            private Type conceptionKind;

            public InsertRecordThread(Type conceptionKind, List<EntityValue> conceptionEntityValueList){
                this.conceptionEntityValueList = conceptionEntityValueList;
                this.conceptionKind = conceptionKind;
            }
            @Override
            public void run(){
                this.conceptionKind.newEntities(conceptionEntityValueList,false);
            }
        }
        List<List<EntityValue>> rsList = Lists.partition(playlistEntityValueList, 10000);

        ExecutorService executor1 = Executors.newFixedThreadPool(rsList.size());

        for (List<EntityValue> currentEntityValueList : rsList) {
            Type type = coreRealm.getType(PlaylistConceptionType);
            InsertRecordThread insertRecordThread = new InsertRecordThread(type, currentEntityValueList);
            executor1.execute(insertRecordThread);
        }
        executor1.shutdown();

        //PART2

        coreRealm.openGlobalSession();

        Type _SongType2 = coreRealm.getType(SongConceptionType);
        QueryParameters queryParameters2_1 = new QueryParameters();
        NullValueFilteringItem defaultFilterItem2_1 = new NullValueFilteringItem(SongId);
        defaultFilterItem2_1.reverseCondition();
        queryParameters2_1.setDefaultFilteringItem(defaultFilterItem2_1);

        List<String> attributeNamesList1 = new ArrayList<>();
        attributeNamesList1.add(SongId);
        EntitiesAttributesRetrieveResult entitiesAttributesRetrieveResult1 = _SongType2.getSingleValueEntityAttributesByAttributeNames(attributeNamesList1,queryParameters2_1);
        List<EntityValue> entityValueList1 = entitiesAttributesRetrieveResult1.getEntityValues();

        Map<String,String> idUIDMapping_Song2 = new HashMap();
        for(EntityValue currentSongEntityValue : entityValueList1){
            String uid = currentSongEntityValue.getEntityUID();
            String idValue = currentSongEntityValue.getEntityAttributesValue().get(SongId).toString();
            idUIDMapping_Song2.put(idValue,uid);
        }

        Type _PlaylistType1 = coreRealm.getType(PlaylistConceptionType);
        QueryParameters queryParameters3 = new QueryParameters();
        NullValueFilteringItem defaultFilterItem3 = new NullValueFilteringItem(PlaylistId);
        defaultFilterItem3.reverseCondition();
        queryParameters3.setDefaultFilteringItem(defaultFilterItem3);
        queryParameters3.setResultNumber(10000000);
        queryParameters3.addSortingAttribute(PlaylistId, QueryParameters.SortingLogic.ASC);

        List<String> attributeNamesList = new ArrayList<>();
        attributeNamesList.add(PlaylistContent);
        EntitiesAttributesRetrieveResult entitiesAttributesRetrieveResult = _PlaylistType1.getSingleValueEntityAttributesByAttributeNames(attributeNamesList,queryParameters3);
        List<EntityValue> entityValueList = entitiesAttributesRetrieveResult.getEntityValues();

        Map<String,String> idContentMapping_Playlist = new HashMap();
        for(EntityValue currentEntityValue : entityValueList){
            String uid = currentEntityValue.getEntityUID();
            String playlistContentValue = currentEntityValue.getEntityAttributesValue().get(PlaylistContent).toString();
            idContentMapping_Playlist.put(uid,playlistContentValue);
        }
        coreRealm.closeGlobalSession();

        Iterator<Map.Entry<String, String>> iterator = idContentMapping_Playlist.entrySet().iterator();
        Map<String, String> mapThread0 = new HashMap<>();
        Map<String, String> mapThread1 = new HashMap<>();
        Map<String, String> mapThread2 = new HashMap<>();
        Map<String, String> mapThread3 = new HashMap<>();
        Map<String, String> mapThread4 = new HashMap<>();
        Map<String, String> mapThread5 = new HashMap<>();
        Map<String, String> mapThread6 = new HashMap<>();
        Map<String, String> mapThread7 = new HashMap<>();
        Map<String, String> mapThread_default = new HashMap<>();

        List<Map<String, String>> mapThreadList = new ArrayList<>();
        mapThreadList.add(mapThread0);
        mapThreadList.add(mapThread1);
        mapThreadList.add(mapThread2);
        mapThreadList.add(mapThread3);
        mapThreadList.add(mapThread4);
        mapThreadList.add(mapThread5);
        mapThreadList.add(mapThread6);
        mapThreadList.add(mapThread7);
        mapThreadList.add(mapThread_default);

        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String key = entry.getKey();
            String.valueOf(key);
            int hashCode = Math.abs(String.valueOf(key).hashCode());

            switch (hashCode % 8) {
                case 0 :
                    mapThread0.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 1 :
                    mapThread1.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 2 :
                    mapThread2.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 3 :
                    mapThread3.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 4 :
                    mapThread4.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 5 :
                    mapThread5.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 6 :
                    mapThread6.put(key, idContentMapping_Playlist.get(key));
                    break;
                case 7 :
                    mapThread7.put(key, idContentMapping_Playlist.get(key));
                    break;
                default:
                    mapThread_default.put(key, idContentMapping_Playlist.get(key));
                    break;
            }
        }

        class LinkPlaylistAndSongThread implements Runnable{

            private Map<String,String> playlistDataMap;
            private Map<String,String> songIdRIDMapping;

            public LinkPlaylistAndSongThread(Map<String,String> playlistDataMap,Map<String,String> songIdRIDMapping){
                this.playlistDataMap = playlistDataMap;
                this.songIdRIDMapping = songIdRIDMapping;
            }

            @Override
            public void run() {
                Engine coreRealm = EngineFactory.getDefaultEngine();
                coreRealm.openGlobalSession();
                Type _PlaylistType = coreRealm.getType(PlaylistConceptionType);

                Set<String> playlistUIDKeySet = this.playlistDataMap.keySet();
                for(String currentUID :playlistUIDKeySet){
                    String uid = currentUID;
                    String playlistContent = this.playlistDataMap.get(uid);

                    String[] dataItems = playlistContent.split(" ");
                    for(String currentSongIdValue:dataItems){
                        String currentSongId = currentSongIdValue.trim();
                        try {
                            Entity currentEntity = _PlaylistType.getEntityByUID(uid);
                            String _songEntityUID = songIdRIDMapping.get(currentSongId);
                            if(_songEntityUID != null){
                                currentEntity.attachToRelation(_songEntityUID,"playedInList",null,true);
                            }

                        } catch (EngineServiceRuntimeException e) {
                            e.printStackTrace();
                        }
                    }
                }
                coreRealm.closeGlobalSession();
            }
        }

        ExecutorService executor2 = Executors.newFixedThreadPool(mapThreadList.size());
        for(Map<String, String> currentMapThread:mapThreadList){
            LinkPlaylistAndSongThread linkPlaylistAndSongThread1 = new LinkPlaylistAndSongThread(currentMapThread,idUIDMapping_Song2);
            executor2.execute(linkPlaylistAndSongThread1);
        }
        executor2.shutdown();

    }

    private static void linkSongToTagItem(Entity _SongEntity, Map<String,String> idUIDMapping_Song, Map<String,String> idUIDMapping_MusicTag,
                                          String musicTagId) throws EngineServiceRuntimeException {
        String _musicTagEntityUID = idUIDMapping_MusicTag.get(musicTagId);
        if(_musicTagEntityUID != null){
            _SongEntity.attachFromRelation(_musicTagEntityUID,"belongsToMusicType",null,true);
        }
    }
}
