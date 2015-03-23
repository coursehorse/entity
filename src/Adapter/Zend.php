<?php
/**
 * Data source adapter for Zend_Db_Table_Abstract
 *
 * @copyright  2014 by CourseHorse Inc.
 * @license    http://mev.com/license.txt
 * @author     Emil Diaz <emil@coursehorse.com>
 */
namespace CourseHorse\Adapter;

use CourseHorse\DataSourceInterface;
use CourseHorse\Entity_Abstract;
use \CourseHorse_Date;
use \CourseHorse_Db_Select;
use \Zend_Db_Table_Abstract;
use \Zend_Loader_Autoloader;
use \ArrayObject;
use \ArrayAccess;
use \ReflectionClass;
use \Exception;

class Zend extends Zend_Db_Table_Abstract implements DataSourceInterface {
    private static $_cacheEnabled = true;
    private static $_localCache = [];
    private static $_reflectCache = [];
    protected static $entityName;

    public function __construct($config = array(), $name = null) {
        if ($name) $config[self::NAME] = $name;
        parent::__construct($config);
    }

    public function getEntity($entityClass, $id, Entity_Abstract $existingEntity = null, $ignoreFields = []) {
        if (!$id) return null;
        if (!$entity = $this->_getFromLocalCache($entityClass, $id)) {
            if(!$row = $this->_selectOne($entityClass::getDataSourceName(), $id, $ignoreFields)) {
                return null;
            }
            $entity = $this->mapEntity($row, $existingEntity, $entityClass);
        }
        return $entity;
    }

    public function getEntities($entityClass, array $ids = [], CourseHorse_Db_Select $select = null) {
        $cachedEntities = [];

        $dsName = $entityClass::getDataSourceName();

        if ($select) {
            // configure query for this type of operation
            $select->from($dsName);
            $select->where($dsName . '.id IN (?)', (array) $ids);
            $select->expand();

            $rows = $this->_query($select);
            $entities = transform_array($select->normalize($rows), function($row, $rowId) {
                return transform_array($row, function($subrows, $tableName) {
                    return $this->mapEntities(new ArrayObject($subrows), self::getEntityClass($tableName));
                });
            });

            return $entities;
        }
        else {
            // check for entities in cache
            $originalIds = $ids;
            foreach($ids as $i => $id) {
                if ($entity = $this->_getFromLocalCache($entityClass, $id)) {
                    $cachedEntities[$entity->id] = $entity;
                    unset($ids[$i]);
                }
            }

            // All entities found in cache
            if (!empty($originalIds) && empty($ids)) return $cachedEntities;

            // load the remaining entities and merged with those found in the cache
            $rows = $this->_select($dsName, !empty($ids) ? ['id IN (?)' => (array) $ids] : []);
            return $cachedEntities + $this->mapEntities(new ArrayObject($rows), $entityClass);
        }
    }

    public function updateEntities(array $entities, $data) {
        if (empty($entities)) return null;
        $ids = [];
        foreach($entities as $entity) {
            $ids[] = $entity->id;
        }
        $this->_update($entity::getDataSourceName(), $ids, $data);
    }

    public function saveEntity(Entity_Abstract $entity) {
        // get data for db
        $data = $this->_mapData($entity);

        // skip of there is nothing to update
        if (empty($data)) return $entity;

        // insert new row
        if (!$entity->id) {
            $this->_insert($entity::getDataSourceName(), $data);
            $entity->id = $this->getAdapter()->lastInsertId();
        }
        // update existing row
        else {
            $this->_update($entity::getDataSourceName(), $entity->id, $data);
        }

        // clear from cache so that the entity can be reloaded from the data source
        $this->_removeFromLocalCache($entity, $entity->id, '*');
        return $this->getEntity(get_class($entity), $entity->id, $entity);
    }

    public function deleteEntity(Entity_Abstract $entity) {
        $this->_delete($entity::getDataSourceName(), ["id = ?" => $entity->id]);
        $this->_removeFromLocalCache($entity, $entity->id, '*');
    }

    public function getDependents($parentClass, $ids, $dependentClass, $where = [], $order = null, $limit = null, $count = false) {
        if (!$ids) return ($limit == 1) ? null : [];

        $whereHash = md5(serialize($where) . serialize($order) . serialize($limit) . serialize($count));

        $cachedDeps = $this->_getFromLocalCache($parentClass, $ids, ['dependents', $dependentClass, $whereHash]);
        if (!is_null($cachedDeps)) return $cachedDeps;

        $select = $this->getAdapter()->select();

        // Add join clause
        if ($linkTableName = $this->_getLinkTable($parentClass, $dependentClass)) {
            $info = $this->_getReferences($linkTableName);
            $dependentKey = $info[$dependentClass::getDataSourceName()]['column'];
            $parentKey = $info[$parentClass::getDataSourceName()]['column'];
            $selectParam = $count ? ['count' => 'count(1)'] : ['*'];
            $select->from(['a' => $dependentClass::getDataSourceName()], $selectParam);
            $select->join(['b' => $linkTableName], "a.id = b.{$dependentKey}", [$parentKey]);
            $select->where("b.{$parentKey} IN (?)", (array) $ids);
            if ($count) $select->group("b.{$parentKey}");
        }
        else {
            $info = $this->_getReferences($dependentClass::getDataSourceName());
            $parentKey = $info[$parentClass::getDataSourceName()]['column'];
            $selectParam = $count ? ['count' => 'count(1)', $parentKey] : ['*'];
            $select->from(['a' => $dependentClass::getDataSourceName()], $selectParam);
            $select->where("a.{$parentKey} IN (?)", (array) $ids);
            if ($count) $select->group("a.{$parentKey}");
        }

        // Add where clause
        foreach ((array) $where as $clause => $value) {
            is_numeric($clause) ? $select->where('a.' . $value) : $select->where('a.' . $clause, $value);
        }

        // Add order clause
        if ($order) {
            $select->order($order);
        }

        // Add limit clause
        $rows = $this->_query($select);
        $entities = [];
        $groups = [];

        foreach($rows as $row) {
            // limit applied to the groups not to the entire query
            if ($limit && count(av($groups, $row[$parentKey], [])) >= $limit) {
                continue;
            }

            if ($count) {
                $groups[$row[$parentKey]] = $row['count'];
                $entities[$row[$parentKey]] = $row['count'];
            } else {
                // individual dependent might already be in the cache
                if (!$entity = $this->_getFromLocalCache($dependentClass, $row['id'])) {
                    $entity = $this->mapEntity($row, null, $dependentClass);
                }

                $entity->_links[$parentClass::getEntityName()][] = $row[$parentKey];
                $groups[$row[$parentKey]][$row['id']] = $entity;
                $entities[$row['id']] = $entity;
            }
        };

        $this->_saveToLocalCache($parentClass, $ids, $entities, ['dependents', $dependentClass, $whereHash]);
        foreach((array) $ids as $id) {
            // If limit == 1 we need to flatten the result to a single entity as opposed to an array
            $groupedEntities = ($limit == 1) ? first(av($groups, $id)) : av($groups, $id);
            $default = $count ? 0 : (($limit == 1) ? null : []);
            $returnableEntities = $groupedEntities ?: $default;
            $this->_saveToLocalCache($parentClass, $id, $groupedEntities ?: $default, ['dependents', $dependentClass, $whereHash]);
        }

        // If only one ID was asked for, return just that group instead of a size-one array of groups
        return count($ids) > 1 ? $entities : $returnableEntities;
    }

    public function addDependent(Entity_Abstract $parent, Entity_Abstract $dependent) {
        if (!$linkTableName = $this->_getLinkTable(get_class($parent), get_class($dependent))) {
            throw new Exception('Cannot determine reference table to add dependent to');
        }

        $info = $this->_getReferences($linkTableName);
        $dependentKey = $info[$dependent::getDataSourceName()]['column'];
        $parentKey = $info[$parent::getDataSourceName()]['column'];

        if ($row = $this->_select($linkTableName, ["$dependentKey = ?" => $dependent->id, "$parentKey = ?" => $parent->id])) {
            throw new Exception(
                "Cannot add dependent {$dependent::getEntityName()}($dependent->id)." .
                "Dependent is already attached to parent {$parent::getEntityName()}($parent->id)."
            );
        }

        $this->_insert($linkTableName, [$dependentKey => $dependent->id, $parentKey => $parent->id]);

        // link could be by-directional so clear both caches
        $this->_removeFromLocalCache($parent, $parent->id, ['dependents', get_class($dependent), '*']);
        $this->_removeFromLocalCache($dependent, $dependent->id, ['dependents', get_class($parent), '*']);
    }

    public function deleteDependent(Entity_Abstract $parent, Entity_Abstract $dependent) {
        if (!$linkTableName = $this->_getLinkTable(get_class($parent), get_class($dependent))) {
            throw new Exception('Cannot determine reference table to remove dependent from');
        }

        $info = $this->_getReferences($linkTableName);
        $dependentKey = $info[$dependent::getDataSourceName()]['column'];
        $parentKey = $info[$parent::getDataSourceName()]['column'];

        if (!$row = $this->_select($linkTableName, ["$dependentKey = ?" => $dependent->id, "$parentKey = ?" => $parent->id])) {
            throw new Exception(
                "Cannot remove dependent {$dependent::getEntityName()}($dependent->id)." .
                "Dependent is not attached to parent {$parent::getEntityName()}($parent->id)."
            );
        }

        $this->_delete($linkTableName, ["$dependentKey = ?" => $dependent->id, "$parentKey = ?" => $parent->id]);

        // link could be by-directional so clear both caches
        $this->_removeFromLocalCache($parent, $parent->id, ['dependents', get_class($dependent), '*']);
        $this->_removeFromLocalCache($dependent, $dependent->id, ['dependents', get_class($parent), '*']);
    }

    public function getAdapter() {
        return $this->_db;
    }

    public function getColumns($table = null) {
        return array_keys($this->_getMetadata($table));
    }

    public static function clearCache() {
        self::$_localCache = [];
    }

    public static function disableCache() {
        self::$_cacheEnabled = false;
    }

    public static function getEntityClass($table) {
        $entityClass = 'Entity_' . str_replace(' ', '', snakeToNormal($table));
        $tableClass = str_replace(' ', '', snakeToNormal($table)) . 'Table';

        // Check if a table class has been defined
        if (is_class($tableClass)) $entityClass = 'Entity_' . $tableClass::$entityName;

        // Check to make sure we found a real entity class
        if (!is_class($entityClass)) throw new Exception("Cannot determine Entity class for table '$table");

        return $entityClass;
    }

    protected function mapEntity($data, Entity_Abstract $entity = null, $entityClass = null) {
        if (!$data) return null;

        $entityClass = $entityClass ?: static::getEntityClass($this->info(self::NAME));
        $entity = $entity ?: $this->_getFromLocalCache($entityClass, $data['id']) ?: new $entityClass();
        $this->_map($data, $entity);
        $this->_saveToLocalCache($entity, $entity->id, $entity);

        return $entity;
    }

    protected function mapEntities(ArrayAccess $rows, $entityClass = null) {
        if (count($rows) == 0)
            return array();

        $entities = array();
        foreach($rows as $row) {
            if (empty($row['id'])) continue;
            $entities[$row['id']] = $this->mapEntity($row, null, $entityClass);
        }
        return $entities;
    }

    private function _map($data, Entity_Abstract $entity) {
        // database -> entity
        $reflection = new ReflectionClass($entity);

        // If $data is a row object, turn into an array first
        if (is_object($data)) {
            $data = $data->toArray();
        }

        $metadatas = $this->_getMetadata($entity::getDataSourceName());

        foreach ($metadatas as $column => $metadata) {
            $value = av($data, $column);
            $type = av($metadata, 'DATA_TYPE');

            // could not map column to entity property
            if (!$property = av($metadata, 'MAP')) {
                continue;
            }

            // initialize date types
            if (in_array($type, ['datetime', 'timestamp', 'date', 'time'])) {
                $value = empty($value) ? null : new CourseHorse_Date($value);
            }

            // initialize float and decimal types
            if (in_array($type, ['decimal'])) {
                $value = is_null($value) ? null : (float) $value;
            }

            // initialize int types
            if (in_array($type, ['int', 'tinyint', 'smallint', 'mediumint', 'bigint'])) {
                $value = is_null($value) ? null : (int) $value;
            }

            $prop = $reflection->getProperty($property);
            $prop->setAccessible(true);
            $prop->setValue($entity, $value);
        }

        $map = (new ReflectionClass($entity))->getMethod('map')->getClosure($entity);
        call_user_func($map, $data);
    }

    private function _mapData(Entity_Abstract $entity) {
        $mapToDataSource = (new ReflectionClass($entity))->getMethod('mapToDataSource')->getClosure($entity);

        // entity -> database
        $data = [];
        foreach ($entity->getDirty() as $property => $value) {
            if ($value instanceof CourseHorse_Date) {
                $value = $value->toString();
            }

            if (is_array($value)) {
                $value = implode(',', $value);
            }

            $scProperty = camelToSnakeCase($property);
            $cscProperty = 'course_' . $scProperty;
            $rnProperty = 'course_' . lcfirst($entity::getEntityName()) . '_' . $scProperty;
            $mappedProperty = null;

            if ($match = call_user_func($mapToDataSource, $property, $value)) {
                $mappedProperty = key($match);
                $data[$mappedProperty] = current($match);
            }
            // _userId -> user_id
            elseif (preg_match('/_(.+)Id$/', $property, $matches)) {
                $relatedEntityId = $value;
                if (is_null($relatedEntityId)) {
                    $relatedEntityId = ($entity->{$matches[1]} ? $entity->{$matches[1]}->id : null);
                }
                $data[$cscProperty] = $relatedEntityId;
                $data[$scProperty] = $relatedEntityId;
                $data[$rnProperty] = $relatedEntityId;
            }
            // id -> id
            else {
                $data[$scProperty] = $value;
            }
        }

        // filter out properties not in the table
        $columns = array_combine($this->getColumns($entity::getDataSourceName()), array_fill(0, count($this->getColumns($entity::getDataSourceName())), null));
        $data = array_intersect_key($data, $columns);

        return $data;
    }

    private function _getLinkTable($parentClass, $dependentClass) {
        $cacheKey = $parentClass . '.link.' . $dependentClass;
        if (($table = $this->_getFromPersistentCache($cacheKey)) === false) {
            $parentReferences = $this->_getDependents($parentClass::getDataSourceName());
            $dependentReferences = $this->_getDependents($dependentClass::getDataSourceName());
            $matches = array_intersect($parentReferences, $dependentReferences);

            if (empty($parentReferences)) {
                $table = null;
            }
            else if (empty($dependentReferences)) {
                $table = null;
            }
            else if (in_array($dependentClass::getDataSourceName(), $parentReferences)) {
                $table = null;
            }
            else if (count($matches) > 1) {
                $matches = array_filter($matches, function ($match) use ($dependentClass){
                    return strpos($match, strtolower($dependentClass::getEntityName())) !== false;
                });
                $table = first(($matches));
            }
            else if (count($matches) == 1) {
                $table = first($matches);
            }

            $this->_saveToPersistentCache($cacheKey, $table);
        }

        return $table;
    }

    private function _query($select) {
        return $this->getAdapter()->fetchAll($select);
    }

    private function _select($table, $where = [], $order = null, $limit = null) {
        $select = $this->getAdapter()->select()->from($table);
        if (!empty($where)) {
            foreach($where as $clause => $value) {
                $select->where($clause, $value);
            }
        }
        if (!empty($order)) $select->order($order);
        if (!empty($limit)) $select->limit($limit);
        return $this->_query($select);
    }

    private function _selectOne($table, $id, $ignoreFields = []) {
        $cols = $this->getColumns($table);
        foreach ($ignoreFields as $field) {
            $key = array_search($field, $cols);
            unset($cols[$key]);
        }

        $select = $this->getAdapter()->select()
            ->from($table, !empty($ignoreFields) ? $cols : '*')
            ->where('id = ?', $id);
        return $this->getAdapter()->fetchRow($select);
    }

    private function _insert($table, $data) {
        return $this->getAdapter()->insert($table, $data);
    }

    private function _update($table, $ids, $data) {
        return $this->getAdapter()->update($table, $data, ['id IN (?)' => (array) $ids]);
    }

    private function _delete($table, $where = []) {
        return $this->getAdapter()->delete($table, $where);
    }

    private function _getMetadata($table = null) {
        $cacheKey = $table.'-metadata';
        if (($metaData = $this->_getFromPersistentCache($cacheKey)) === false) {
            // tricking the parent class into thinking
            // that it needs to load the metadata
            $originalMetadata = $this->_metadata;
            $originalName = $this->_name;
            $this->_name = $table ?: $originalName;
            $this->_metadata = null;
            parent::_setupMetadata();
            $metaData = $this->_metadata;
            $this->_name = $table ?: $originalName;
            $this->_metadata = $originalMetadata;

            $entityClass = $this->getEntityClass($table);
            if (!$properties = av(self::$_reflectCache, $entityClass . '_properties')) {
                $reflection = new ReflectionClass($entityClass);
                $properties = self::$_reflectCache[$entityClass . '_properties'] = transform_array($reflection->getProperties(), 'name');
            }

            foreach ($properties as $property) {
                $column = $property;
                $scColumn = camelToSnakeCase($property);
                $cscColumn = 'course_' . $scColumn;
                $rnColumn = 'course_' . lcfirst($entityClass::getEntityName()) . '_' . $scColumn;

                // id -> id
                if (array_key_exists($column, $metaData)) {
                    $metaData[$column]['MAP'] = $property;
                }
                // _userId -> user_id , allowSyndication -> allow_syndication
                elseif (array_key_exists($scColumn, $metaData)) {
                    $metaData[$scColumn]['MAP'] = $property;
                }
                // _uriId -> course_uri_id
                elseif (array_key_exists($cscColumn, $metaData)) {
                    $metaData[$cscColumn]['MAP'] = $property;
                }
                // type -> course_category_type_id
                elseif (array_key_exists($rnColumn, $metaData)) {
                    $metaData[$rnColumn]['MAP'] = $property;
                }
            }

            $this->_saveToPersistentCache($cacheKey, $metaData);
        }

        return $metaData;
    }

    private function _getReferences($table) {
        $cacheKey = $table.'-references';
        if (($data = $this->_getFromPersistentCache($cacheKey)) === false) {
            $select = $this->getAdapter()->select()
                ->from('INFORMATION_SCHEMA.KEY_COLUMN_USAGE', ['COLUMN_NAME', 'REFERENCED_TABLE_NAME', 'REFERENCED_COLUMN_NAME'])
                ->where('TABLE_SCHEMA = DATABASE()')
                ->where('TABLE_NAME = ?', $table);
            $rows = $this->_query($select);
            $data = extract_pairs($rows, 'REFERENCED_TABLE_NAME', function($row) {
                return [
                    'column'    => $row['COLUMN_NAME'],
                    'refTable'  => $row['REFERENCED_TABLE_NAME'],
                    'refColumn' => $row['REFERENCED_COLUMN_NAME']
                ];
            }, false);
            $this->_saveToPersistentCache($cacheKey, $data);
        }

        return $data;
    }

    private function _getDependents($table) {
        $cacheKey = $table.'-dependents';
        if (($data = $this->_getFromPersistentCache($cacheKey)) === false) {
            $select = $this->getAdapter()->select()
                ->from('INFORMATION_SCHEMA.KEY_COLUMN_USAGE', ['TABLE_NAME'])
                ->where('REFERENCED_TABLE_SCHEMA = DATABASE()')
                ->where('REFERENCED_TABLE_NAME = ?', $table);
            $rows = $this->_query($select);
            $data = transform_array($rows, 'TABLE_NAME');
            $this->_saveToPersistentCache($cacheKey, $data);
        }

        return $data;
    }

    private function _getPersistentCache() {
        // If $this has no metadata cache but the class has a default metadata cache
        if (null === $this->_metadataCache && null !== self::$_defaultMetadataCache) {
            $this->_setMetadataCache(self::$_defaultMetadataCache);
        }
        return $this->_metadataCache;
    }

    private function _getFromPersistentCache($key) {
        if (!$cache = $this->_getPersistentCache()) return null;

        // build the cacheId = port:host/dbname:schema.key  key usually is the table name plus an identifier
        $dbConfig = $this->getAdapter()->getConfig();
        $port = av($dbConfig['options'], 'port') ?: av($dbConfig, 'port');
        $host = av($dbConfig['options'], 'host') ?: av($dbConfig, 'host');
        $cacheId = $port.':'.$host.'/'.$dbConfig['dbname'].':'.$this->_schema.':'.$key;
        $cacheId = md5($cacheId);

        // load from local cache
        if (self::$_cacheEnabled && array_key_exists($cacheId, self::$_localCache)) {
            return av(self::$_localCache, $cacheId);
        }

        return $this->_metadataCache->load($cacheId);
    }

    private function _saveToPersistentCache($key, $data) {
        if (!$cache = $this->_getPersistentCache()) return;

        // build the cacheId = port:host/dbname:schema.key  key usually is the table name plus an identifier
        $dbConfig = $this->getAdapter()->getConfig();
        $port = av($dbConfig['options'], 'port') ?: av($dbConfig, 'port');
        $host = av($dbConfig['options'], 'host') ?: av($dbConfig, 'host');
        $cacheId = md5($port.':'.$host.'/'.$dbConfig['dbname'].':'.$this->_schema.':'.$key);

        // save to local cache as well
        if (self::$_cacheEnabled) {
            self::$_localCache[$cacheId] = $data;
        }

        $this->_metadataCache->save($data, $cacheId);
    }

    private function _getFromLocalCache($entityClass, $id, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return null;
        $key = $this->_getKey($entityClass, $id, $additionalKeys);

        return av(self::$_localCache, $key);
    }

    private function _saveToLocalCache($entityClass, $id, $data, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return;
        $key = $this->_getKey($entityClass, $id, $additionalKeys);

        self::$_localCache[$key] = $data;
    }

    private function _removeFromLocalCache($entityClass, $id, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return;
        $entityClass = is_object($entityClass) ? get_class($entityClass) : $entityClass;

        // wildcard matches
        if ($additionalKeys == '*') {
            // unset the exact key match (need to ignore additionalKeys for this)
            $key = $this->_getKey($entityClass, $id);
            unset(self::$_localCache[$key]);

            // unset wildcard matches ex: Entity_Course.123.dependents[.Entity_Section].whereHash
            $key = '.' . $entityClass;
            foreach(self::$_localCache as $id => $data) {
                if (in($key, $id)) unset(self::$_localCache[$id]);
            }
        }
        // targeted wildcard matches
        elseif (last((array) $additionalKeys) == '*') {
            $additionalKeys = (array) $additionalKeys;
            array_pop($additionalKeys);

            // unset wildcard matches within a domain ex: [Entity_Course.123.dependents].Entity_Section.whereHash
            $key = $this->_getKey($entityClass, $id, $additionalKeys);
            foreach(self::$_localCache as $id => $data) {
                if (in($key, $id)) unset(self::$_localCache[$id]);
            }
        }
        // exact match
        else {
            // unset the exact key match
            $key = $this->_getKey($entityClass, $id, $additionalKeys);
            unset(self::$_localCache[$key]);
        }
    }

    private function _getKey($entityClass, $ids, $additionalKeys = []) {
        if (is_array($ids)) array_walk($ids, 'intval');
        if (is_numeric($ids)) $ids = (int) $ids;
        $entityClass = is_object($entityClass) ? get_class($entityClass) : $entityClass;
        return implode('.', array_merge([$entityClass, serialize($ids)], (array) $additionalKeys));
    }



    #TEMPORARY WHILE I REMOVE TAU DEPENDENCIES FROM COURSEHOURSE
    public $_rowsetClass = 'Tau_Db_Table_Rowset_Abstract';

    final public function getRowByAnchor() {
        $anchor = func_get_args();
        if (count($anchor) == 1) {
            if ($anchor[0] instanceof $this->_rowClass) {
                $RowGateway = $anchor[0];
            } else {
                $RowGateway = $this->find($anchor[0])
                    ->current();
            }
        } else {
            $RowGateway = call_user_func_array(array($this, 'find'), $anchor)
                ->current();
        }

        if (!$RowGateway) {
            return null;
            //throw new Tau_Exception('Anchor Problem: No active row');
        }

        return $RowGateway;
    }

    public function getSimpleDataList($field_name='caption', $order=null, $where=null) {
        if(!is_array($field_name)) {
            $field_name = array('id', $field_name);
        }

        $db = $this->getDefaultAdapter();
        $select = $db->select()
            ->from($this->_name, $field_name)
        ;
        if($where) {
            $select->where($where);
        }
        if($order) {
            $select->order($order);
        }

        return $db->fetchAll($select);
    }

    public function addRowFromArray($data) {
        $RowGateway = $this->createRow();
        return $RowGateway->updateRowData($data);
    }

    public function updateRowDataByAnchor($row, $data) {
        if ($RowGateway = $this->getRowByAnchor($row)) {
            return $RowGateway->updateRowData($data);
        }
    }

    public function deleteRowByAnchor($row) {
        if ($RowGateway = $this->getRowByAnchor($row)) {
            $RowGateway->delete();
        }
    }

    public function clearFromCache($entityClass, $id) {
        $this->_removeFromLocalCache($entityClass, $id, '*');
    }
}