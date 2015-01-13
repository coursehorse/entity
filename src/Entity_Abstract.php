<?php
/**
 * Base Class for all entities.
 *
 * @copyright  2014 by CourseHorse Inc.
 * @license    http://mev.com/license.txt
 * @author     Emil Diaz <emil@coursehorse.com>
 */
namespace CourseHorse;

use CourseHorse\Adapter\Zend;
use \CourseHorse_Date;
use \CourseHorse_Db_Select;
use \ReflectionClass;
use \Exception;

abstract class Entity_Abstract {
    public $_links = [];
    private $_snapshot = [];
    private $_dependentEntities = [];

    protected static $_dependents = [];
    protected static $_table;


    public function __construct(array $data = []) {
        $this->_setArray($data);
        $this->_snapshot();
    }

    public function __toString() {
        if (isset($this->name)) {
            return $this->name;
        }
        elseif (isset($this->caption)) {
            return $this->caption;
        }
        elseif (isset($this->uri)) {
            return $this->uri;
        }
        else
            return "";
    }

    public function __set($name, $value = null) {
        $methodName = 'set'.ucfirst($name);
        $propName = '_'.$name.'Id';
        if (method_exists($this, $methodName)) {
            call_user_func_array(array($this, $methodName),array($value));
        }
        elseif (property_exists($this, $propName)) {
            $this->setRelatedEntityProperty($name, $value);
        }
        elseif(preg_match('/.+Date$/', $name) || preg_match('/^date$/', $name)) { // Can be delete after replace all date strings to CourseHorse_Date object
            $this->_setDateField($name, $value);
        }
        elseif(preg_match('/.+Time$/', $name) || preg_match('/^time$/', $name)) {
            $this->_setDateField($name, $value);
        }
        elseif (property_exists($this, $name)) {
            $this->$name = $value;
        }
        else {
            throw new Exception("Unknown property '$name'");
        }
    }

    public function __get($name) {
        $methodName = 'get'.ucfirst($name);
        $propName = '_'.$name;
        $propIdName = '_'.$name.'Id';

        // Explicit getter always has highest precedence
        if (method_exists($this, $methodName)) {
            $value = call_user_func(array($this, $methodName));
            if (!array_key_exists($name, $this->_dependentEntities)) {
                $this->setRelatedEntityProperty($name, $value);
            }
            return $value;
        }
        // Next if property ID exists load related entity
        elseif (property_exists($this, $propIdName)) {
            $entityClass = is_class('Entity_'.ucfirst($name)) ? 'Entity_'.ucfirst($name) : get_class($this).ucfirst($name);
            return $this->getRelatedEntity($name, $entityClass);
        }
        // Next if dependent config exists load dependent
        elseif (!empty(static::$_dependents[$name])) {
            return $this->getDependent($name);
        }
        // Next load special protected properties
        elseif (property_exists($this, $propName)) {
            return $this->$propName;
        }
        // Next load normal protected properties
        elseif (property_exists($this, $name)) {
            return $this->$name;
        }
        // Unknown property
        else {
            throw new Exception("Unknown property '$name'");
        }
    }

    public function __isset($name) {
        $methodName = 'get'.ucfirst($name);
        $propName = '_'.$name;
        $propIdName = '_'.$name.'Id';

        if (method_exists($this, $methodName)) {
            return true;
        }
        elseif (property_exists($this, $propIdName)) {
            return true;
        }
        elseif (!empty(static::$_dependents[$name])) {
            return true;
        }
        elseif (property_exists($this, $propName)) {
            return true;
        }
        elseif (property_exists($this, $name)) {
            return true;
        }
        else {
            return false;
        }
    }

    public function __call($name, array $arguments) {
        $methodName = 'get'.ucfirst($name);
        if (method_exists($this, $methodName)) {
            return call_user_func_array(array($this, $methodName), $arguments);
        }
        else {
            return self::__callStatic($name, $arguments);
        }
    }

    public function __sleep() {
        return ['id'];
    }

    public function __wakeup() {
        // Reload failed so let's clear the entity and log the warning
        if ($this->id && !$this->reload()) {
            throw new Exception(get_class($this) . " with ID ({$this->id}) does not exist.");
        }
    }

    public function save(array $data = []) {
        $this->_setArray($data);
        $isNew = (bool) !$this->id;
        $this->preSave();
        static::getDataSource()->saveEntity($this);
        $isNew ? $this->postInsert() : $this->postUpdate();
        $isNew ? $this->_notifyReferences('dependentAdded') : $this->_notifyReferences('dependentUpdated');
        $this->_snapshot();
    }

    public function drop() {
        $this->preDelete();
        static::getDataSource()->deleteEntity($this);
        $this->postDelete();
        $this->_notifyReferences('dependentRemoved');
    }

    public function reload() {
        return static::getDataSource()->getEntity(get_called_class(), $this->id, $this);
    }

    public function copy(Entity_Abstract $entity) {
        if (get_class($this) !== get_class($entity))
            throw new Exception('Can\'t copy different objects');
        foreach (get_object_vars($entity) as $key => $value) {
            if ($key == 'id') continue;
            $this->$key = $value;
        }
    }

    public function addDependent(Entity_Abstract $dependent) {
        $this->getDataSource()->addDependent($this, $dependent);
        $this::dependentAdded($this->id, $dependent);
        $dependent::dependentAdded($dependent->id, $this);
    }

    public function deleteDependent(Entity_Abstract $dependent) {
        $this->getDataSource()->deleteDependent($this, $dependent);
        $this::dependentRemoved($this->id, $dependent);
        $dependent::dependentRemoved($dependent->id, $this);
    }

    public function toArray($options = null) {
        $values = [];
        $reflection = new ReflectionClass($this);
        $properties = $reflection->getProperties();
        $properties = transform_array($properties, 'name');

        foreach($properties as $property) {
            // Ignore these properties
            if (in_array($property, ['_table', '_dependents', '_snapshot', '_dependentEntities', '_links'])) continue;

            // Flatten IDs
            if ($property[0] == '_') {
                $property = substr($property, 1);
            }

            // Snake case names
            $property[0] = strtolower($property[0]);
            $scProperty = preg_replace_callback('/([A-Z])/', function($matches) {
                return '_' . strtolower($matches[0]);
            }, $property);

            $values[$scProperty] = $this->__get($property);

            if ($values[$scProperty] instanceof CourseHorse_Date) {
                $values[$scProperty] = isset($options['date_format']) ?
                    $values[$scProperty]->toString($options['date_format']) :
                    $values[$scProperty]->toString();
            }
        }

        return $values;
    }

    public function getSnapshot() {
        $properties = $this->id ?
            array_diff_assoc($this->_properties(), $this->_snapshot) :
            $this->_snapshot;

        unset($properties['id']);
        return $properties;
    }

    public function getDirty() {
        $properties = $this->id ?
            array_diff_assoc($this->_snapshot, $this->_properties()) :
            array_filter($this->_properties(), function($var) { return !is_null($var); });

        unset($properties['id']);
        return $properties;
    }

    public function callHook($name) {
        #HACK to support hooks with zend db rows
        $this->{$name};
    }

    public static function __callStatic($name, $arguments) {
        // Check for an entity loader method
        if (strpos($name, 'getAll') !== false) {
            $methodName = 'getEntities'.ucfirst(substr($name,6));
        }
        elseif (strpos($name, 'getBy') !== false) {
            $methodName = 'getEntityBy'.ucfirst(substr($name,5));
        }
        else {
            $methodName = $name;
        }

        if (!method_exists(static::getDataSource(), $methodName)) {
            throw new Exception("Unknown entity loading method '$name'");
        }

        return call_user_func_array(array(static::getDataSource(), $methodName), $arguments);
    }

    public static function load($ids) {
        $ds = static::getDataSource();
        return $ds->getEntities(get_called_class(), (array) $ids);
    }

    public static function loadBulk($ids, $eagerFetchProperties = [], $context = null, $contextData = null) {
        # Eager load using SQL joins

        $ds = static::getDataSource();

        $select = new CourseHorse_Db_Select($ds);

        $eagerFetchEntities = array_intersect_key(
            array_merge(static::_getReferenceProperties(), static::_getDependentProperties()),
            $eagerFetchProperties
        );
        $eagerFetchTables = transform_array($eagerFetchEntities, function($class) {
            return $class::getDataSourceName();
        });

        foreach ($eagerFetchProperties as $property => $on) {
            if (empty($eagerFetchTables[$property])) {
                throw new Exception("Invalid eager loading configuration. Path '$property' is not configured for " . get_called_class());
            }

            $entityClass = $eagerFetchEntities[$property];
            $tableName = $eagerFetchTables[$property];

            // Process sub load call
            if (is_array($on)) {
                list($subeagerFetchProperties, $subIds) = $on;
                $entityClass::loadBulk($subIds, $subeagerFetchProperties, get_called_class().'.'.$property, $ids);
                unset($eagerFetchEntities[$property]);
                unset($eagerFetchTables[$property]);
                continue;
            }

            // honor dependency rules
            $where = null;
            $joinWhere = '';
            if (!empty(static::$_dependents[$property])) {
                list($type, $where, $order, $limit) = self::_getDependentConfig($property);
                foreach((array) $where as $clause => $value) {
                    if (is_string($clause)) {
                        $joinWhere = " AND ($tableName." . $select->getAdapter()->quoteInto($clause, $value) . ")";
                    }
                    else {
                        $joinWhere = " AND ($tableName." . $value . ")";
                    }
                }
            }

            $select->joinLeft($tableName, $on . $joinWhere);
        }

        // This is being loaded in the context of another entity
        if ($context) {
            list($entityClass, $property) = explode('.', $context);

            if (!empty($entityClass::_getDependentProperties()[$property])) {
                list($type, $where, $order, $limit) = $entityClass::_getDependentConfig($property);
                foreach((array) $where as $clause => $value) {
                    $select->where(static::getDataSourceName() . '.' . $clause, $value);
                }
                if ($order) $select->order($order);
            }
        }

        $mainDataSourceName = static::getDataSourceName();
        $rows = $ds->getEntities(get_called_class(), (array) $ids, $select);
        $entities = [];

        foreach ($rows as $i => $row) {
            $entity = first(av($row, $mainDataSourceName));

            foreach ($entity::_getReferenceProperties() as $property => $class) {
                // reference was not part of eager loading request
                if (empty($eagerFetchEntities[$property])) continue;

                // reference was not loaded
                if (!$referenceEntities = av($row, $class::getDataSourceName())) continue;

                // this specific reference was not loaded
                if (!$referenceEntity = av($referenceEntities, $entity->{$property.'Id'})) continue;

                $entity->$property = $referenceEntity;
            }

            foreach ($entity::_getDependentProperties() as $property => $class) {
                // reference was not part of eager loading request
                if (empty($eagerFetchEntities[$property])) continue;

                // reference was not loaded
                if (!array_key_exists($class::getDataSourceName(), $row)) continue;

                $dependentEntities = av($row, $class::getDataSourceName());

                list($type, $where, $order, $limit) = $entity::_getDependentConfig($property);

                if ($limit ==  1) {
                    $entity->setRelatedEntityProperty($property, first($dependentEntities));
                }
                else {
                    $entity->setRelatedEntityProperty($property, $dependentEntities + av($entity->_dependentEntities, $property, []));
                }
            }

            $entities[$entity->id] = $entity;
        }

        // This is being loaded in the context of another entity so CACHE IT
        if ($context) {
            list($entityClass, $property) = explode('.', $context);

            if (!empty($entityClass::_getDependentProperties()[$property])) {
                list($type, $where, $order, $limit) = $entityClass::_getDependentConfig($property);
                $childClass = 'Entity_' . $type;
                if (!$key = av(array_flip($childClass::_getReferenceProperties()), $entityClass)) {
                    throw new Exception("Invalid eager loading configuration. Path '$property' does not reference " . get_called_class());
                }

                foreach((array) $contextData as $id) {
                    $children = array_filter($entities, function($entity) use($id, $key) {
                        return $entity->{$key.'Id'} == $id;
                    });

                    $ds->cache($entityClass, $id, $children ?: [], $childClass, $where, $order, $limit);
                }
            }
        }

        return $entities;
    }

    public static function loadProgressive($ids, $eagerFetchProperties = [], $context = null, $contextData= null) {
        # Eager load in progressive selects

        $ds = static::getDataSource();

        // Otherwise use progressive loading
        if ($context) {
            list($type, $where, $order, $limit) = self::_getDependentConfig($context);
            $entities = call_user_func_array([$ds, 'getDependents'], [get_called_class(), (array) $ids, 'Entity_' . $type, $where, $order, $limit]);
        }
        else {
            $entities = $ds->getEntities(get_called_class(), (array) $ids);
        }

        // Eager load properties relying on good old recursion to traverse
        // the property path. Ignore if no entities available to recurs
        if (!empty($entities)) {
            foreach((array) $eagerFetchProperties as $propertyPath) {
                if (!$propertyPath) continue;

                $propertyPathParts = explode('.', $propertyPath);
                $currentPathPart = array_shift($propertyPathParts);
                @list($currentPathPart, $hint) = explode(':', $currentPathPart);
                $propertyPath = implode('.', $propertyPathParts);

                // Load one -> one dependency
                $entityName = 'Entity_' . ucfirst($hint ?: $currentPathPart);
                if (property_exists(first($entities), '_' . $currentPathPart . 'Id')) {
                    $ids = array_unique(transform_array($entities, '_' . $currentPathPart . 'Id'));
                    $children = $entityName::loadProgressive($ids, [$propertyPath]);
                    array_walk($entities, function($entity) use($currentPathPart, $children) {
                        if ($child = av($children, $entity->{'_' . $currentPathPart . 'Id'})) {
                            $entity->{$currentPathPart} = $child;
                        }
                    });
                    continue;
                }

                // Load one -> many or many -> many dependency
                $entityName = !empty($type) ? 'Entity_' . $type : get_called_class();
                $vars = get_class_vars($entityName);
                if (!empty($vars['_dependents'][$currentPathPart])) {
                    $ids = array_unique(transform_array($entities, 'id'));
                    $children = $entityName::loadProgressive($ids, [$propertyPath], $currentPathPart);
                    $groupedChildren = [];
                    foreach($children as $child) {
                        foreach($child->_links[$entityName::getEntityName()] as $link) {
                            $groupedChildren[$link][] = $child;
                        }
                    }
                    $limit = av($vars['_dependents'][$currentPathPart], 3);
                    array_walk($entities, function($entity) use($currentPathPart, $groupedChildren, $limit) {
                        $children = av($groupedChildren, $entity->id, []);
                        $entity->setRelatedEntityProperty($currentPathPart, $limit == 1 ? first($children) : $children);
                    });
                    continue;
                }

                throw new Exception("Invalid eager loading configuration. Path '$currentPathPart' is not configured for " . get_called_class());

            }
        }

        return $entities;
    }

    public static function get($id, $ignoreFields = []) {
        return static::getDataSource()->getEntity(get_called_class(), $id, null, $ignoreFields);
    }

    public static function getAll() {
        return static::getDataSource()->getEntities(get_called_class());
    }

    public static function getEntityName() {
        return substr(get_called_class(), 7);
    }

    public static function update(array $entities, array $data = []) {
        return static::getDataSource()->updateEntities($entities, $data);
    }

    public static function getDataSourceName() {
        $name = static::$_table ? substr(static::$_table, 0, -5) : static::getEntityName();
        return camelToSnakeCase($name);
    }

    public static function callStaticHook($name, $id, Entity_Abstract $dependent) {
        #HACK to support hooks with zend db rows
        static::$$name($id, $dependent);
    }


    protected function map($data) {}

    protected function mapData() {}

    protected function preSave() {}

    protected function postInsert() {}

    protected function postUpdate() {}

    protected function preDelete() {}

    protected function postDelete() {}

    protected function getDependent($name, array $additionalWhere = []) {
        if (array_key_exists($name, $this->_dependentEntities)) {
            $whereHash = md5(serialize($additionalWhere));
            if (!empty($additionalWhere) && array_key_exists($whereHash, $this->_dependentEntities[$name])) {
                return $this->_dependentEntities[$name][$whereHash];
            }

            return $this->_dependentEntities[$name];
        }

        list($type, $where, $order, $limit) = self::_getDependentConfig($name);

        $entities = $this->getDataSource()->getDependents(get_class($this), $this->id, 'Entity_' . $type, array_merge($where ?: [], $additionalWhere), $order, $limit);
        $value = $limit == 1 ? first($entities) : $entities;
        $this->setRelatedEntityProperty($name, $value);

        return $value;
    }

    protected function getRelatedEntity($field, $entityName) {
        $idField = '_' . $field . 'Id';
        if (!empty($this->$idField)) {
            // If the entity collector is empty
            if (empty($this->_dependentEntities[$field])) {
                $this->setRelatedEntityProperty($field, $entityName::get($this->$idField));
            }
            // If the entity collector is stale, reload
            elseif ($this->_dependentEntities[$field]->id != $this->$idField) {
                $this->setRelatedEntityProperty($field, $entityName::get($this->$idField));
            }
        }

        return av($this->_dependentEntities, $field, []);
    }

    protected function setRelatedEntityProperty($field, $value) {
        $idField = '_' . $field . 'Id';
        if (property_exists($this, $idField)) {
            if ($value instanceof Entity_Abstract) {
                $this->_dependentEntities[$field] = $value;
                $this->$idField = $value->id;
            }
            else if (is_numeric($value) || is_null($value)){
                unset($this->_dependentEntities[$field]);
                $this->$idField = $value;
            }
        }
        else {
            $this->_dependentEntities[$field] = $value;
        }
    }

    protected static function getDataSource() {
        $_table = static::$_table ?: 'CourseHorse\\Adapter\\Zend';
        return new $_table([Zend::NAME => static::getDataSourceName()]);
    }

    protected static function dependentAdded($id, Entity_Abstract $dependent) {}

    protected static function dependentUpdated($id, Entity_Abstract $dependent) {}

    protected static function dependentRemoved($id, Entity_Abstract $dependent) {}


    private function _properties() {
        $data = [];
        $reflection = new ReflectionClass($this);
        $properties = $reflection->getProperties();
        foreach ($properties as $property) {
            $name = $property->name;

            // ignore static properties
            if ($property->isStatic()) continue;

            // ignore these specific properties
            if (in_array($name, ['_snapshot', '_dependentEntities', '_links'])) continue;

            $value = $this->$name;

            // Special handling for dates
            if ($value instanceof CourseHorse_Date) {
                $value = $value->toString();
            }

            $data[$name] = $value;
        }

        return $data;
    }

    private function _snapshot() {
        $this->_snapshot = $this->_properties();
    }

    private function _setArray(array $data = []) {
        foreach($data as $key => $value) {
            $this->__set($key, $value);
        }
    }

    private function _setDateField($field, $value) {
        if (empty($value) || $value instanceof CourseHorse_Date) {
            $this->$field = $value;
        }
        elseif(is_string($value)) {
            $this->$field = new CourseHorse_Date($value);
        }
    }

    private function _notifyReferences($type) {
        // One-to-Many Relationships (direct references)
        foreach($this::_getReferenceProperties() as $name => $class) {
            call_user_func_array([$class, $type], [$this->{$name.'Id'}, $this]);
        }

        // Many-to-Many Relationships (linked references)
        foreach($this::_getDependentProperties() as $name => $class) {
            foreach($this->{$name} as $dependent) {
                call_user_func_array([$class, $type], [$dependent->id, $this]);
            }
        }
    }

    private static function _getDependentConfig($name) {
        $config = [null, null, null, null];
        if (empty(static::$_dependents[$name])) {
            throw new Exception("invalid eager loading configuration. path '$name' is not configured for " . get_called_class());
        }
        return $config = static::$_dependents[$name] + $config;;
    }

    private static function _getReferenceProperties() {
        $thisClass = get_called_class();
        $values = [];
        $reflection = new ReflectionClass($thisClass);
        $properties = $reflection->getProperties();
        $properties = transform_array($properties, 'name');

        foreach($properties as $property) {
            // Flatten IDs
            if (($property[0] == '_') && (substr($property, -2) == 'Id')) {
                $name = substr($property, 1, -2);
                $class = null;
                if (is_class('Entity_'.ucfirst($name))) $class = 'Entity_'.ucfirst($name);
                if (is_class($thisClass.ucfirst($name))) $class = $thisClass.ucfirst($name);
                if (!$class) continue;
                $values[$name] = $class;
            }
        }

        return $values;
    }

    private static function _getDependentProperties() {
        $thisClass = get_called_class();
        return extract_pairs(static::$_dependents,
            function($config, $dependent) use($thisClass) {
                return in('#', $dependent) ? substr($dependent, 1) : $dependent;
            },
            function($config, $dependent) use ($thisClass) {
                $class = null;
                $name = $config[0];
                if (is_class('Entity_'.ucfirst($name))) $class = 'Entity_'.ucfirst($name);
                if (is_class($thisClass.ucfirst($name))) $class = $thisClass.ucfirst($name);
                return $class;
            },
            false
        );
    }
}