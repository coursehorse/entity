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

class Zend extends Zend_Db_Table_Abstract implements DataSourceInterface {
    private static $_cacheEnabled = true;
    private static $_localCache = [];
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

    public function getEntities($entityClass, array $ids = []) {
        $cachedEntities = [];

        foreach($ids as $i => $id) {
            if ($entity = $this->_getFromLocalCache($entityClass, $id)) {
                $cachedEntities[$entity->id] = $entity;
                unset($ids[$i]);
            }
        }

        $rows = $this->_select($entityClass::getDataSourceName(), !empty($ids) ? ['id IN (?)' => (array) $ids] : []);

        return $cachedEntities + $this->mapEntities(new ArrayObject($rows), $entityClass);
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
        // get table columns and entity data
        $defaults = array_combine($this->_getColumns(), array_fill(0, count($this->_getColumns()), null));
        $data = $this->_mapData($entity);

        // never allow IDs to be updated
        unset($data['id']);

        // filter out properties not in the table
        $data = array_intersect_key($data, $defaults);

        // skip of there is nothing to update
        if (empty($data)) return $entity;

        // insert new row
        if (!$entity->id) {
            $this->_insert($entity::getDataSourceName(), $data);
            $entity->id = $this->_adapter()->lastInsertId();
        }
        // update existing row
        else {
            $this->_update($entity::getDataSourceName(), $entity->id, $data);
        }

        return $this->getEntity(get_class($entity), $entity->id, $entity);
    }

    public function deleteEntity(Entity_Abstract $entity) {
        $this->_delete($entity::getDataSourceName(), ["id = ?" => $entity->id]);
        $this->_removeFromLocalCache($entity, $entity->id, '*');
    }

    public function getDependents($parentClass, $ids, $dependentClass, $where = [], $order = null, $limit = null) {
        if (!$ids) return null;

        $whereHash = md5(serialize($where) . serialize($order) . serialize($limit));

        if ($this->_isInLocalCache($parentClass, $ids, ['dependents', $dependentClass, $whereHash])) {
            return $this->_getFromLocalCache($parentClass, $ids, ['dependents', $dependentClass, $whereHash]);
        }

        $select = $this->_adapter()->select()->from(['a' => $dependentClass::getDataSourceName()]);

        // Add join clause
        if ($linkTableName = $this->_getLinkTable($parentClass, $dependentClass)) {
            $info = $this->_getReferences($linkTableName);
            $dependentKey = $info[$dependentClass::getDataSourceName()]['column'];
            $parentKey = $info[$parentClass::getDataSourceName()]['column'];
            $select->join(['b' => $linkTableName], "a.id = b.{$dependentKey}", [$parentKey]);
            $select->where("b.{$parentKey} IN (?)", (array) $ids);
        }
        else {
            $info = $this->_getReferences($dependentClass::getDataSourceName());
            $parentKey = $info[$parentClass::getDataSourceName()]['column'];
            $select->where("a.{$parentKey} IN (?)", (array) $ids);
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
            $entity = $this->mapEntity($row, null, $dependentClass);
            $entity->_links[$parentClass::getEntityName()][] = $row[$parentKey];
            $groups[$row[$parentKey]][] = $entity;
            $entities[$row['id']] = $entity;
        };

        $this->_saveToLocalCache($parentClass, $ids, $entities, ['dependents', $dependentClass, $whereHash]);
        foreach((array) $ids as $id) {
            $this->_saveToLocalCache($parentClass, $id, av($groups, $id, []), ['dependents', $dependentClass, $whereHash]);
        }

        return $entities;
    }

    public function addDependent(Entity_Abstract $parent, Entity_Abstract $dependent) {
        if (!$linkTableName = $this->_getLinkTable($parent, $dependent)) {
            throw new CourseHorse_Exception('Cannot determine reference table to add dependent to');
        }

        $info = $this->_getReferences($linkTableName);
        $dependentKey = $info[$dependent::getDataSourceName()]['column'];
        $parentKey = $info[$parent::getDataSourceName()]['column'];

        if ($row = $this->_select($linkTableName, ["$dependentKey = ?" => $dependent->id, "$parentKey = ?" => $parent->id])) {
            throw new CourseHorse_Exception('Cannot add dependent. Dependent already attached.');
        }

        $this->_insert($linkTableName, [$dependentKey => $dependent->id, $parentKey => $parent->id]);
        $this->_removeFromLocalCache($parent, $parent->id, 'dependents');
    }

    public function deleteDependent(Entity_Abstract $parent, Entity_Abstract $dependent) {
        if (!$linkTableName = $this->_getLinkTable($parent, $dependent)) {
            throw new CourseHorse_Exception('Cannot determine reference table to remove dependent from');
        }

        $info = $this->_getReferences($linkTableName);
        $dependentKey = $info[$dependent::getDataSourceName()]['column'];
        $parentKey = $info[$parent::getDataSourceName()]['column'];

        if (!$row = $this->_select($linkTableName, ["$dependentKey = ?" => $dependent->id, "$parentKey = ?" => $parent->id])) {
            throw new CourseHorse_Exception('Cannot remove dependent. Dependent is not attached.');
        }

        $this->_delete($linkTableName, ["$dependentKey = ?" => $dependent->id, "$parentKey = ?" => $parent->id]);
        $this->_removeFromLocalCache($parent, $parent->id, 'dependents');
    }

    public static function clearCache() {
        self::$_localCache = [];
    }

    public static function disableCache() {
        self::$_cacheEnabled = false;
    }

    public static function getEntityClass() {
        return 'Entity_' . static::$entityName;
    }

    protected function mapEntity($data, Entity_Abstract $entity = null, $entityClass = null) {
        if (!$data) return null;

        $entityClass = $entityClass ?: static::getEntityClass();
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
            $entities[$row['id']] = $this->mapEntity($row, null, $entityClass);
        }
        return $entities;
    }

    private function _map($data, Entity_Abstract $entity) {
        // database -> entity
        $reflection = new ReflectionClass($entity);
        $properties = $reflection->getProperties();

        // If $data is a row object, turn into an array first
        if (is_object($data)) {
            $data = $data->toArray();
        }

        foreach ($properties as $property) {
            $propertyString = $property->name;
            $scProperty = camelToSnakeCase($propertyString);
            $cscProperty = 'course_' . $scProperty;
            $rnProperty = 'course_' . lcfirst($entity::getEntityName()) . '_' . $scProperty;

            // id -> id
            if (array_key_exists($propertyString, $data)) {
                $entity->$propertyString = $data[$propertyString];
            }
            // _userId -> user_id
            // allowSyndication -> allow_syndication
            elseif (array_key_exists($scProperty, $data)) {
                $entity->$propertyString = $data[$scProperty];
            }
            // _uriId -> course_uri_id
            elseif (array_key_exists($cscProperty, $data)) {
                $entity->$propertyString = $data[$cscProperty];
            }
            // type -> course_category_type_id
            elseif (array_key_exists($rnProperty, $data)) {
                $entity->$propertyString = $data[$rnProperty];
            }
        }

        $map = $reflection->getMethod('map')->getClosure($entity);
        call_user_func($map, $data);
    }

    private function _mapData(Entity_Abstract $entity) {
        // entity -> database
        $reflection = new ReflectionClass($entity);
        $properties = $reflection->getProperties();

        $data = [];
        foreach ($properties as $property) {
            $propertyString = $property->name;

            // Skip all static properties ($_table)
            if ($property->isStatic()) {
                continue;
            }

            $scProperty = camelToSnakeCase($propertyString);
            $cscProperty = 'course_' . $scProperty;
            $rnProperty = 'course_' . lcfirst($entity::getEntityName()) . '_' . $scProperty;

            // subscriptionDate -> subscription_date
            if (preg_match('/.+Date$/', $propertyString) || preg_match('/^date$/', $propertyString) || preg_match('/.+Time$/', $propertyString) || preg_match('/^time$/', $propertyString)) {
                $data[$scProperty] = $entity->$propertyString ? $entity->$propertyString->toString() : null;
            }
            // _userId -> user_id
            elseif (preg_match('/_(.+)Id$/', $propertyString, $matches)) {
                $relatedEntityId = $entity->$propertyString ?: ($entity->{$matches[1]} ? $entity->{$matches[1]}->id : null);
                $data[$cscProperty] = $relatedEntityId;
                $data[$scProperty] = $relatedEntityId;
                $data[$rnProperty] = $relatedEntityId;
            }
            // id -> id
            else {
                $data[$scProperty] = $entity->$propertyString;
            }
        }

        $map = $reflection->getMethod('mapData')->getClosure($entity);
        $data = (call_user_func($map, $data) ?: []) + $data;

        return $data;
    }

    private function _getLinkTable($parentClass, $dependentClass) {
        $parentReferences = $this->_getDependents($parentClass::getDataSourceName());
        $dependentReferences = $this->_getDependents($dependentClass::getDataSourceName());

        if (empty($parentReferences)) return null;

        if (empty($dependentReferences)) return null;

        if (in_array($dependentClass::getDataSourceName(), $parentReferences)) return null;

        $matches = array_intersect($parentReferences, $dependentReferences);

        if (count($matches) > 1) {
            $matches = array_filter($matches, function ($match) use ($dependentClass){
                return strpos($match, strtolower($dependentClass::getEntityName())) !== false;
            });
        }

        return first(($matches));
    }

    private function _adapter() {
        return $this->_db;
    }

    private function _query($select) {
        return $this->_adapter()->fetchAll($select);
    }

    private function _select($table, $where = [], $order = null, $limit = null) {
        $select = $this->_adapter()->select()->from($table);
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
        $cols = $this->_getColumns();
        foreach ($ignoreFields as $field) {
            unset($cols[$field]);
        }

        $select = $this->_adapter()->select()
            ->from($table, !empty($ignoreFields) ? $cols : '*')
            ->where('id = ?', $id);
        return $this->_adapter()->fetchRow($select);
    }

    private function _insert($table, $data) {
        return $this->_adapter()->insert($table, $data);
    }

    private function _update($table, $ids, $data) {
        return $this->_adapter()->update($table, $data, ['id IN (?)' => (array) $ids]);
    }

    private function _delete($table, $where = []) {
        return $this->_adapter()->delete($table, $where);
    }

    private function _getColumns() {
        return array_keys($this->_getMetadata());
    }

    private function _getMetadata($table = null) {
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

        return $metaData;
    }

    private function _getReferences($table) {
        $cacheKey = $table.'-references';
        if (($data = $this->_getFromPersistentCache($cacheKey)) === false) {
            $select = $this->_adapter()->select()
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
            $select = $this->_adapter()->select()
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
        $dbConfig = $this->_adapter()->getConfig();
        $port = av($dbConfig['options'], 'port') ?: av($dbConfig, 'port');
        $host = av($dbConfig['options'], 'host') ?: av($dbConfig, 'host');
        $cacheId = md5($port.':'.$host.'/'.$dbConfig['dbname'].':'.$this->_schema.':'.$key);

        return $this->_metadataCache->load($cacheId);
    }

    private function _saveToPersistentCache($key, $data) {
        if (!$cache = $this->_getPersistentCache()) return;

        // build the cacheId = port:host/dbname:schema.key  key usually is the table name plus an identifier
        $dbConfig = $this->_adapter()->getConfig();
        $port = av($dbConfig['options'], 'port') ?: av($dbConfig, 'port');
        $host = av($dbConfig['options'], 'host') ?: av($dbConfig, 'host');
        $cacheId = md5($port.':'.$host.'/'.$dbConfig['dbname'].':'.$this->_schema.':'.$key);

        $this->_metadataCache->save($data, $cacheId);
    }

    private function _getFromLocalCache($entityClass, $id, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return null;
        $key = $this->_getKey($entityClass, $id, $additionalKeys);

        return av(self::$_localCache, $key);
    }

    private function _saveToLocalCache($entityClass, $id, $data, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return null;
        $key = $this->_getKey($entityClass, $id, $additionalKeys);

        self::$_localCache[$key] = $data;
    }

    private function _removeFromLocalCache($entityClass, $id, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return null;
        $key = $this->_getKey($entityClass, $id, $additionalKeys);

        // remove wildcard matches
        // ex: parent_table.dependents.dependent_table.id.*
        if (substr($key, -1) == '*') {
            foreach(self::$_localCache as $id => $data) {
                $key = substr($key, 0, -1);
                if (in($key, $id)) unset(self::$_localCache);
            }
        }
        unset(self::$_localCache[$key]);
    }

    private function _isInLocalCache($entityClass, $id, $additionalKeys = []) {
        if (!self::$_cacheEnabled) return null;
        $key = $this->_getKey($entityClass, $id, $additionalKeys);

        return array_key_exists($key, self::$_localCache);
    }

    private function _getKey($entityClass, $id, $additionalKeys = []) {
        return implode('.', array_merge([$entityClass::getEntityName(), serialize($id)], (array) $additionalKeys));
    }
}