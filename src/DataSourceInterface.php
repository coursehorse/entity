<?php
/**
 * Interface for data sources
 *
 * @copyright  2014 by CourseHorse Inc.
 * @license    http://mev.com/license.txt
 * @author     Emil Diaz <emil@coursehorse.com>
 */
namespace CourseHorse;

interface DataSourceInterface {

    public function getEntity($entityClass, $id, Entity_Abstract $existingEntity = null);

    public function getEntities($entityClass, array $ids = []);

    public function updateEntities(array $entities, $data);

    public function saveEntity(Entity_Abstract $entity);

    public function deleteEntity(Entity_Abstract $entity);

    public function getDependents($entityClass, $ids, $dependentName, $where = [], $order = null, $limit = null);

    public function addDependent(Entity_Abstract $parent, Entity_Abstract $dependent);

    public function deleteDependent(Entity_Abstract $parent, Entity_Abstract $dependent);

    public static function clearCache();

    public static function disableCache();
} 