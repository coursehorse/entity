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
use \ReflectionClass;
use \Exception;

abstract class Entity_Abstract {
    public $_links = [];
    private $_references = [];
    private $_reflectProperties = [];

    private static $_reflectCache = [];
    protected static $_table;
    protected static $_maps = [];
    protected static $_dependents = [];
    protected static $_sources = [];

    /**
     * __construct
     *
     * @param array $data Optional For setting properties of the new object
     *
     * @return void
     */
    public function __construct(array $data = []) {
        $this->_setArray($data);
    }

    /**
     * __toString
     *
     * Looks for a few specially named properties that might contain a descripion of the entity
     *
     * @return string
     */
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
        else {
            return "";
        }
    }

    /**
     * __set
     *
     * The magic behind setting properties and relationships. If there's a custom property setter method defined, prioritize that.
     *
     * @return void
     */
    public function __set($name, $value = null) {
        $methodName = 'set' . ucfirst($name);
        $propName = '_' . $name . 'Id';
        $lowerNameEnd = strtolower(substr($name, -4));

        if (method_exists($this, $methodName)) {
            call_user_func_array([$this, $methodName], [$value]);
        }
        elseif (property_exists($this, $propName)) {
            $this->setRelatedEntityProperty($name, $value);
        }
        elseif ($lowerNameEnd === 'time' || $lowerNameEnd === 'date') { // Can be delete after replace all date strings to CourseHorse_Date object
            $this->_setDateField($name, $value);
        }
        elseif (property_exists($this, $name)) {
            $this->$name = $value;
        }
        else {
            throw new Exception("Unknown property '$name'");
        }
    }

    /**
     * __get
     *
     * The magic behind getting properties and relationships. If there's a custom property getter method defined, prioritize that.
     *
     * @return void
     */
    public function __get($name) {
        $methodName = 'get' . ucfirst($name);
        $propName = '_' . $name;
        $propIdName = '_' . $name . 'Id';

        // Explicit getter always has highest precedence
        if (method_exists($this, $methodName)) {
            return call_user_func([$this, $methodName]);
        }
        // Next if property ID exists load related entity
        elseif (property_exists($this, $propIdName)) {
            $entityClass = is_class('Entity_' . ucfirst($name)) ? 'Entity_' . ucfirst($name) : get_class($this) . ucfirst($name);
            return $this->getRelatedEntity($name, $entityClass);
        }
        // Next if dependent config exists load dependent
        elseif (static::_hasDependentConfig($name)) {
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

    /**
     * __isset
     *
     * Checks whether a property or relationship exists on this entity
     *
     * @return void
     */
    public function __isset($name) {
        $methodName = 'get' . ucfirst($name);
        $propName = '_' . $name;
        $propIdName = '_' . $name . 'Id';

        if (method_exists($this, $methodName)) {
            return true;
        }
        elseif (property_exists($this, $propIdName)) {
            return true;
        }
        elseif (static::_hasDependentConfig($name)) {
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

    /**
     * __call
     *
     * Allows calling get* methods by shorter names
     *
     * @return void
     */
    public function __call($name, array $arguments) {
        $methodName = 'get' . ucfirst($name);

        if (method_exists($this, $methodName)) {
            return call_user_func_array([$this, $methodName], $arguments);
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

    /**
     * save
     *
     * Save the entity to the data source with all callbacks
     *
     * @param array $data Optional data to update before actually saving
     *
     * @return void
     */
    public function save(array $data = []) {
        $this->_setArray($data);
        $isNew = (bool) !$this->id;
        $this->preSave();
        $isNew ? $this->preInsert() : $this->preUpdate();
        static::getDataSource()->saveEntity($this);
        $isNew ? $this->postInsert() : $this->postUpdate();
        $isNew ? $this->_notifyReferences('dependentAdded') : $this->_notifyReferences('dependentUpdated');
    }

    /**
     * drop
     *
     * Delete the entity from the data source with all callbacks
     *
     * @return void
     */
    public function drop() {
        $this->preDelete();
        static::getDataSource()->deleteEntity($this);
        $this->postDelete();
        $this->_notifyReferences('dependentRemoved');
    }

    /**
     * reload
     *
     * Re-fetch the entity data from the data source and overwrite all properties and relationships
     *
     * @return Entity_Abstract
     */
    public function reload() {
        static::getDataSource()->clearFromCache(get_called_class(), $this->id);
        return static::getDataSource()->getEntity(get_called_class(), $this->id, $this);
    }

    /**
     * copy
     *
     * Set all the properties on this entity (except ID) from values of the passed in entity
     *
     * @param Entity_Abstract $entity Entity to copy properties FROM
     *
     * @return void
     */
    public function copy(Entity_Abstract $entity) {
        if (get_class($this) !== get_class($entity)) {
            throw new Exception('Can\'t copy different objects');
        }

        foreach (get_object_vars($entity) as $key => $value) {
            if ($key == 'id') continue;

            $this->$key = $value;
        }
    }

    /**
     * addDependent
     *
     * Create a link between two entities in the data source and notify callbacks
     *
     * @param Entity_Abstract $dependent
     *
     * @return void
     */
    public function addDependent(Entity_Abstract $dependent) {
        static::getDataSource()->addDependent($this, $dependent);
        $this::dependentAdded($this->id, $dependent);
        $dependent::dependentAdded($dependent->id, $this);
    }

    /**
     * deleteDependent
     *
     * Remove a link between two entities in the data source and notify callbacks
     *
     * @param Entity_Abstract $dependent
     *
     * @return void
     */
    public function deleteDependent(Entity_Abstract $dependent) {
        static::getDataSource()->deleteDependent($this, $dependent);
        $this::dependentRemoved($this->id, $dependent);
        $dependent::dependentRemoved($dependent->id, $this);
    }

    /**
     * toArray
     *
     * Convert the entity to an array of property values, re-formatting some keys
     *
     * @param $options
     *
     * @return array
     */
    public function toArray($options = null) {
        $values = [];
        $properties = $this->_reflectProperties();

        foreach($properties as $name) {
            // Flatten IDs
            if ($name[0] == '_') {
                $name = substr($name, 1);
            }

            // Snake case names
            $name[0] = strtolower($name[0]);
            $scProperty = preg_replace_callback('/([A-Z])/', function($matches) {
                return '_' . strtolower($matches[0]);
            }, $name);

            $values[$scProperty] = $this->__get($name);

            if ($values[$scProperty] instanceof CourseHorse_Date) {
                $values[$scProperty] = isset($options['date_format']) ? $values[$scProperty]->toString($options['date_format']) : $values[$scProperty]->toString();
            }
        }

        return $values;
    }

    /**
     * getDirty
     *
     * Return data to be saved to the data source, stripping out nulls
     *
     * @return array
     */
    public function getDirty() {
        $properties = $this->id ? $this->_properties() : array_clear_nulls($this->_properties());
        unset($properties['id']);
        return $properties;
    }

    /**
     * callHook
     *
     * HACK to support hooks with zend db rows
     *
     * @param $name
     *
     * @return void
     */
    public function callHook($name) {
        call_user_func([$this, $name]);
    }

    /**
     * __callStatic
     *
     * Allows calling shorter/modified versions of static loading methods. Eg Entity_*::getAllByStatus instead of
     * Entity_*::getEntitiesByStatus
     *
     * @param $name
     * @param $arguments
     *
     * @return void
     */
    public static function __callStatic($name, $arguments) {
        // Check for an entity loader method
        if (strpos($name, 'getAll') !== false) {
            $methodName = 'getEntities' . ucfirst(substr($name, 6));
        }
        elseif (strpos($name, 'getBy') !== false) {
            $methodName = 'getEntityBy' . ucfirst(substr($name, 5));
        }
        else {
            $methodName = $name;
        }

        if (!method_exists(static::getDataSource(), $methodName)) {
            throw new Exception("Unknown entity loading method '$name'");
        }

        return call_user_func_array([static::getDataSource(), $methodName], $arguments);
    }

    /**
     * load
     *
     * A shortcut for getting one or more entities by ID
     *
     * @param $ids
     *
     * @return array
     */
    public static function load($ids) {
        $ids = (array) $ids;
        if (empty($ids)) {
            return [];
        }

        $ds = static::getDataSource();
        return $ds->getEntities(get_called_class(), $ids);
    }

    /**
     * loadProgressive
     *
     * Load one or more entities and their relationships in a recursive fashion
     *
     * @param $ids
     * @param array $eagerFetchProperties Optional An array of relationships to load, which can be nested if separated by "."s
     * @param $context Optional
     * @param $contextData Optional
     *
     * @return array
     */
    public static function loadProgressive($ids, $eagerFetchProperties = [], $context = null, $contextData = []) {
        $ids = (array) $ids;
        if (empty($ids)) {
            return [];
        }

        # Eager load in progressive selects
        $ds = static::getDataSource();

        // Otherwise use progressive loading
        if ($context) {
            list($type, $where, $order, $limit, $count) = self::_getDependentConfig($context);

            // Add additional contextData if present
            $where = $contextData + $where;
            $entities = call_user_func_array([$ds, 'getDependents'], [get_called_class(), $ids, 'Entity_' . $type, $where, $order, $limit, $count]);

            // Need to make sure this is an array of objects or just null
            $entities = is_object($entities) ? [$entities] : $entities;
        }
        else {
            $entities = $ds->getEntities(get_called_class(), $ids);
        }

        // Eager load properties relying on good old recursion to traverse
        // the property path. Ignore if no entities available to recurs
        if (!empty($entities)) {
            foreach((array) $eagerFetchProperties as $key => $propertyPath) {
                // adjust for associative entries
                $propertyOptions = [];
                if (is_array($propertyPath)) {
                    $propertyOptions = $propertyPath;
                    $propertyPath = $key;
                }

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
                    // This will warm the data store caches
                    $entityName::loadProgressive($ids, [$propertyPath], $currentPathPart, $propertyOptions);
                    continue;
                }

                throw new Exception("Invalid eager loading configuration. Path '$currentPathPart' is not configured for " . get_called_class());

            }
        }

        return $entities;
    }

    /**
     * get
     *
     * Fetch and map an entity from the data source
     *
     * @param $id
     * @param array $ignoreFields Optional Properties to unset before returning
     *
     * @return Entity_Abstract
     */
    public static function get($id, $ignoreFields = []) {
        return static::getDataSource()->getEntity(get_called_class(), $id, null, $ignoreFields);
    }

    /**
     * getAll
     *
     * Fetch and map all entities for this class from the data source
     *
     * @return array
     */
    public static function getAll() {
        return static::getDataSource()->getEntities(get_called_class());
    }

    /**
     * getEntityName
     *
     * The class name of the entity without the "Entity_"
     *
     * @return string
     */
    public static function getEntityName() {
        return substr(get_called_class(), 7);
    }

    /**
     * update
     *
     * Save one or more entities without invoking callbacks
     *
     * @param array $entities
     * @param array $data Optional Associative array of property/values to update
     *
     * @return void
     */
    public static function update(array $entities, array $data = []) {
        return static::getDataSource()->updateEntities($entities, $data);
    }

    /**
     * getDataSourceName
     *
     * The name of the data source table where this entity's data is saved
     *
     * @return string
     */
    public static function getDataSourceName() {
        $class = get_called_class();
        if (!$source = av(static::$_sources, $class)) {
            static::$_sources[$class] = $source = camelToSnakeCase(static::$_table ? substr(static::$_table, 0, -5) : static::getEntityName());
        }
        return $source;
    }

    /**
     * callStaticHook
     *
     * HACK to support hooks with zend db rows
     *
     * @return void
     */
    public static function callStaticHook($name, $id, Entity_Abstract $dependent) {
        call_user_func([get_called_class(), $name], $id, $dependent);
    }

    /**
     * map
     *
     * Called after loading the entity's data and setting all of its properties. Can be used to set extra properties
     * whose formats were not picked up by the auto-mapping logic
     *
     * @param array $data From the data source
     *
     * @return void
     */
    protected function map($data) {}

    /**
     * mapToDataSource
     *
     * If any custom manipulation needs to happen before the data is saved to the data source
     *
     * @param $property
     * @param $value
     *
     * @return array
     */
    protected function mapToDataSource($property, $value) {
        // no custom mapping for this property
        if (!array_key_exists($property, static::$_maps)) return;

        return [static::$_maps[$property] => $value];
    }

    /**
     * preSave
     *
     * Called before an entity is saved, whether it's new or not
     *
     * @return void
     */
    protected function preSave() {}

    /**
     * preInsert
     *
     * Called before an entity is inserted
     *
     * @return void
     */
    protected function preInsert() {}

    /**
     * postInsert
     *
     * Called after an entity is inserted
     *
     * @return void
     */
    protected function postInsert() {}

    /**
     * preUpdate
     *
     * Called before an entity is updated
     *
     * @return void
     */
    protected function preUpdate() {}

    /**
     * postUpdate
     *
     * Called after an entity is updated
     *
     * @return void
     */
    protected function postUpdate() {}

    /**
     * preDelete
     *
     * Called before an entity is deleted
     *
     * @return void
     */
    protected function preDelete() {}

    /**
     * postDelete
     *
     * Called after an entity is deleted
     *
     * @return void
     */
    protected function postDelete() {}

    /**
     * getDependent
     *
     * Load a has_many relaionship to this entity by config name
     *
     * @param $name
     * @param array $additionalWhere Optional Filter conditions to add to the existing dependent config
     *
     * @return array
     */
    protected function getDependent($name, array $additionalWhere = []) {
        // this will verify that a config exists for this dependent
        list($type, $where, $order, $limit, $count) = self::_getDependentConfig($name);

        $entities = static::getDataSource()->getDependents(
            get_class($this),
            $this->id,
            'Entity_' . $type,
            array_merge($where ?: [], $additionalWhere),
            $order,
            $limit,
            $count
        );

        return $entities;
    }

    /**
     * getRelatedEntity
     *
     * Load and cache a depends_on realtionship entity
     *
     * @param $field
     * @param $entityName
     *
     * @return mixed
     */
    protected function getRelatedEntity($field, $entityName) {
        $idField = '_' . $field . 'Id';
        if (!empty($this->$idField)) {
            // If the entity collector is empty
            if (empty($this->_references[$field])) {
                $this->setRelatedEntityProperty($field, $entityName::get($this->$idField));
            }
            // If the entity collector is stale, reload
            elseif ($this->_references[$field]->id != $this->$idField) {
                $this->setRelatedEntityProperty($field, $entityName::get($this->$idField));
            }
        }

        return av($this->_references, $field);
    }

    /**
     * setRelatedEntityProperty
     *
     * Cache a depends_on realtionship entity
     *
     * @param $field
     * @param $value
     *
     * @return void
     */
    protected function setRelatedEntityProperty($field, $value) {
        $idField = '_' . $field . 'Id';
        if (property_exists($this, $idField)) {
            if ($value instanceof Entity_Abstract) {
                $this->_references[$field] = $value;
                $this->$idField = $value->id;
            }
            else if (is_numeric($value) || is_null($value)){
                unset($this->_references[$field]);
                $this->$idField = $value;
            }
        }
    }

    /**
     * getDataSource
     *
     * @return DataSourceInterface
     */
    protected static function getDataSource() {
        $_table = static::$_table ?: 'CourseHorse\\Adapter\\Zend';
        return new $_table([Zend::NAME => static::getDataSourceName()]);
    }

    /**
     * dependentAdded
     *
     * Called after a dependent is added
     *
     * @param $id
     * @param Entity_Abstract $dependent
     *
     * @return void
     */
    protected static function dependentAdded($id, Entity_Abstract $dependent) {}

    /**
     * dependentUpdated
     *
     * Called after a dependent is updated
     *
     * @param $id
     * @param Entity_Abstract $dependent
     *
     * @return void
     */
    protected static function dependentUpdated($id, Entity_Abstract $dependent) {}

    /**
     * dependentRemoved
     *
     * Called after a dependent is removed
     *
     * @param $id
     * @param Entity_Abstract $dependent
     *
     * @return void
     */
    protected static function dependentRemoved($id, Entity_Abstract $dependent) {}

    /**
     * _reflectProperties
     *
     * Use PHP reflection to get (and cache, since using reflection is expensive) a list of property names for this object
     *
     * @return array
     */
    private function _reflectProperties() {
        if (!empty($this->_reflectProperties)) return $this->_reflectProperties;

        if (!$properties = av(self::$_reflectCache, get_called_class() . '_properties')) {
            $properties = [];

            foreach ((new ReflectionClass($this))->getProperties() as $property) {
                // ignore static properties
                if ($property->isStatic()) continue;

                // ignore private properties
                if ($property->isPrivate()) continue;

                $name = $property->name;

                // ignore these specific properties
                if (in_array($name, ['_references', '_links', '_reflectProperties'])) continue;
                $properties[] = $name;
            }
        }

        self::$_reflectCache[get_called_class() . '_properties'] = $this->_reflectProperties = $properties;

        return $this->_reflectProperties;
    }

    /**
     * _properties
     *
     * A list of property names and values for this object
     *
     * @return array
     */
    private function _properties() {
        $data = [];
        $properties = $this->_reflectProperties();

        foreach ($properties as $name) {
            $value = $this->$name;

            // Special handling for dates
            if ($value instanceof CourseHorse_Date) {
                $value = $value->toString();
            }

            // Special handling for *_Id properties
            // id field is null but entity exist, synchronize
            if (!$value && ($reference = av($this->_references, substr($name, strpos($name, '_') + 1, -2)))) {
                $this->$name = $reference->id;
                $value = $reference->id;
            }

            $data[$name] = $value;
        }

        return $data;
    }

    /**
     * _setArray
     *
     * Shortcut to set multiple values for this entity
     *
     * @param array $data Optional
     *
     * @return void
     */
    private function _setArray(array $data = []) {
        foreach($data as $key => $value) {
            $this->__set($key, $value);
        }
    }

    /**
     * _setDateField
     *
     * Make sure a date field is set as a CourseHorse_Date, not a string
     *
     * @param $field
     * @param $value
     *
     * @return void
     */
    private function _setDateField($field, $value) {
        if (empty($value) || $value instanceof CourseHorse_Date) {
            $this->$field = $value;
        }
        elseif(is_string($value)) {
            $this->$field = new CourseHorse_Date($value);
        }
    }

    /**
     * _notifyReferences
     *
     * Invoke callbacks by method name
     *
     * @param $type
     *
     * @return void
     */
    private function _notifyReferences($type) {
        // One-to-Many Relationships (direct references)
        foreach($this->_getReferenceProperties() as $name => $class) {
            if (empty($this->{$name.'Id'})) continue;
            call_user_func_array([$class, $type], [$this->{$name.'Id'}, $this]);
        }

        // Many-to-Many Relationships (linked references)
        foreach($this::_getDependentProperties() as $name => $class) {
            if (!empty(static::$_dependents['#'.$name])) {
                foreach($this->{$name} as $dependent) {
                    call_user_func_array([$class, $type], [$dependent->id, $this]);
                }
            }
        }
    }

    /**
     * _hasDependentConfig
     *
     * Does this entity class have a config for this relationship?
     *
     * @param $name
     *
     * @return boolean
     */
    private static function _hasDependentConfig($name) {
        if (!empty(static::$_dependents[$name])) return true;
        if (!empty(static::$_dependents['#'.$name])) return true;
        return false;
    }

    /**
     * _getDependentConfig
     *
     * Get config for this relationship
     *
     * @param $name
     *
     * @return array
     */
    private static function _getDependentConfig($name) {
        $config = [null, [], null, null, false];
        if (!static::_hasDependentConfig($name)) {
            throw new Exception("invalid eager loading configuration. path '$name' is not configured for " . get_called_class());
        }
        if (!empty(static::$_dependents[$name])) return static::$_dependents[$name] + $config;
        if (!empty(static::$_dependents['#' . $name])) return static::$_dependents['#' . $name] + $config;
    }

    /**
     * _getReferenceProperties
     *
     * Get property names that point to a 1-1 entity relationship
     *
     * @return array
     */
    private function _getReferenceProperties() {
        $thisClass = get_called_class();
        $values = [];
        $properties = $this->_reflectProperties();

        foreach($properties as $property) {
            // Flatten IDs
            if (($property[0] == '_') && (substr($property, -2) == 'Id')) {
                $name = substr($property, 1, -2);
                $class = null;
                if (is_class('Entity_' . ucfirst($name))) $class = 'Entity_' . ucfirst($name);
                if (is_class($thisClass . ucfirst($name))) $class = $thisClass . ucfirst($name);
                if (!$class) continue;
                $values[$name] = $class;
            }
        }

        return $values;
    }

    /**
     * _getDependentProperties
     *
     * Get config names that point to a has_many entity relationship
     *
     * @return array
     */
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
