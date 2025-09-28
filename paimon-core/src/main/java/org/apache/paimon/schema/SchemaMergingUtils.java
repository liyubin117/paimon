/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.schema;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/** The util class for merging the schemas.
 * paimon schema evolution 的核心工具类
 * 主要职责是比较两个 schema（通常是数据表的现有 schema 和新写入数据的 schema），并根据预设的规则将它们合并成一个新的、统一的 schema。这个过程支持添加新列、安全地转换现有列的数据类型，从而实现动态 schema 的能力
 * 当配置 Paimon 表允许 schema 合并（例如通过 write.merge-schema=true）时，写入流程就会调用这个工具类
 * */
public class SchemaMergingUtils {

    /**
     * 入口方法，用于合并一个完整的表 schema 和一个新的行类型（通常来自要写入的数据）
     * @param currentTableSchema 当前的 TableSchema 对象。它包含了字段、分区键、主键、表配置等所有元数据
     * @param targetType 目标 RowType，即新数据的 schema
     * @param allowExplicitCast 一个布尔标志，决定是否允许显式（可能存在精度损失）的类型转换，比如 STRING 转 INT
     */
    public static TableSchema mergeSchemas(
            TableSchema currentTableSchema, RowType targetType, boolean allowExplicitCast) {
        RowType currentType = currentTableSchema.logicalRowType();
        // 如果相同，则无需合并，直接返回当前的 TableSchema
        if (currentType.equals(targetType)) {
            return currentTableSchema;
        }

        AtomicInteger highestFieldId = new AtomicInteger(currentTableSchema.highestFieldId());
        // 递归地合并两个 RowType
        RowType newRowType =
                mergeSchemas(currentType, targetType, highestFieldId, allowExplicitCast);
        if (newRowType.equals(currentType)) {
            // It happens if the `targetType` only changes `nullability` but we always respect the
            // current's.
            return currentTableSchema;
        }
        // 如果 schema 确实发生了变化，它会创建一个新的 TableSchema 实例。这个新 schema 的 ID 会在之前所有字段 ID 最大值的基础上加 1，字段列表和 highestFieldId 会更新，而分区键、主键、表配置和注释等信息则会从旧 schema 中继承
        return new TableSchema(
                currentTableSchema.id() + 1,
                newRowType.getFields(),
                highestFieldId.get(),
                currentTableSchema.partitionKeys(),
                currentTableSchema.primaryKeys(),
                currentTableSchema.options(),
                currentTableSchema.comment());
    }

    public static RowType mergeSchemas(
            RowType tableSchema,
            RowType dataSchema,
            AtomicInteger highestFieldId,
            boolean allowExplicitCast) {
        return (RowType) merge(tableSchema, dataSchema, highestFieldId, allowExplicitCast);
    }

    /**
     * Merge the base data type and the update data type if possible.
     *
     * <p>For RowType, find the fields which exists in both the base schema and the update schema,
     * and try to merge them by calling the method iteratively; remain those fields that are only in
     * the base schema and append those fields that are only in the update schema.
     *
     * <p>For other complex type, try to merge the element types.
     *
     * <p>For primitive data type, we treat that's compatible if the original type can be safely
     * cast to the new type.
     */
    public static DataType merge(
            DataType base0,
            DataType update0,
            AtomicInteger highestFieldId,
            boolean allowExplicitCast) {
        // Here we try to merge the base0 and update0 without regard to the nullability,
        // and set the base0's nullability to the return's.
        DataType base = base0.copy(true);
        DataType update = update0.copy(true);

        if (base.equals(update)) {
            return base0;
        } else if (base instanceof RowType && update instanceof RowType) {
            List<DataField> baseFields = ((RowType) base).getFields();
            List<DataField> updateFields = ((RowType) update).getFields();
            Map<String, DataField> updateFieldMap =
                    updateFields.stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            // 合并现有字段: 遍历 base (旧 schema) 的所有字段。对于每个字段，检查 update (新 schema) 中是否存在同名字段。如果存在，就递归调用 merge 方法来合并这两个字段的类型。如果不存在，则保留 base 中的原始字段
            List<DataField> updatedFields =
                    baseFields.stream()
                            .map(
                                    baseField -> {
                                        if (updateFieldMap.containsKey(baseField.name())) {
                                            DataField updateField =
                                                    updateFieldMap.get(baseField.name());
                                            DataType updatedDataType =
                                                    merge(
                                                            baseField.type(),
                                                            updateField.type(),
                                                            highestFieldId,
                                                            allowExplicitCast);
                                            return new DataField(
                                                    baseField.id(),
                                                    baseField.name(),
                                                    updatedDataType,
                                                    baseField.description(),
                                                    baseField.defaultValue());
                                        } else {
                                            return baseField;
                                        }
                                    })
                            .collect(Collectors.toList());

            Map<String, DataField> baseFieldMap =
                    baseFields.stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            // 添加新字段: 遍历 update 的所有字段，找出在 base 中不存在的字段。这些就是需要新增的列。对于每个新字段，调用 assignIdForNewField 为其分配一个新的、唯一的字段 ID，然后将其添加到最终的字段列表中
            List<DataField> newFields =
                    updateFields.stream()
                            .filter(field -> !baseFieldMap.containsKey(field.name()))
                            .map(field -> assignIdForNewField(field, highestFieldId))
                            .map(field -> field.copy(true))
                            .collect(Collectors.toList());

            updatedFields.addAll(newFields);
            return new RowType(base0.isNullable(), updatedFields);
        } else if (base instanceof MapType && update instanceof MapType) {
            return new MapType(
                    base0.isNullable(),
                    merge(
                            ((MapType) base).getKeyType(),
                            ((MapType) update).getKeyType(),
                            highestFieldId,
                            allowExplicitCast),
                    merge(
                            ((MapType) base).getValueType(),
                            ((MapType) update).getValueType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof ArrayType && update instanceof ArrayType) {
            return new ArrayType(
                    base0.isNullable(),
                    merge(
                            ((ArrayType) base).getElementType(),
                            ((ArrayType) update).getElementType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof MultisetType && update instanceof MultisetType) {
            return new MultisetType(
                    base0.isNullable(),
                    merge(
                            ((MultisetType) base).getElementType(),
                            ((MultisetType) update).getElementType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof DecimalType && update instanceof DecimalType) {
            if (((DecimalType) base).getScale() == ((DecimalType) update).getScale()) {
                return new DecimalType(
                        base0.isNullable(),
                        Math.max(
                                ((DecimalType) base).getPrecision(),
                                ((DecimalType) update).getPrecision()),
                        ((DecimalType) base).getScale());
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Failed to merge decimal types with different scale: %s and %s",
                                base, update));
            }
        } else if (supportsDataTypesCast(base, update, allowExplicitCast)) {
            if (DataTypes.getLength(base).isPresent() && DataTypes.getLength(update).isPresent()) {
                // this will check and merge types which has a `length` attribute, like BinaryType,
                // CharType, VarBinaryType, VarCharType.
                if (allowExplicitCast
                        || DataTypes.getLength(base).getAsInt()
                                <= DataTypes.getLength(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a smaller length: %s and %s",
                                    base, update));
                }
            } else if (DataTypes.getPrecision(base).isPresent()
                    && DataTypes.getPrecision(update).isPresent()) {
                // this will check and merge types which has a `precision` attribute, like
                // LocalZonedTimestampType, TimeType, TimestampType.
                if (allowExplicitCast
                        || DataTypes.getPrecision(base).getAsInt()
                                <= DataTypes.getPrecision(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a lower precision: %s and %s",
                                    base, update));
                }
            } else {
                return update.copy(base0.isNullable());
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Failed to merge data types %s and %s", base, update));
        }
    }

    private static boolean supportsDataTypesCast(
            DataType sourceType, DataType targetType, boolean allowExplicitCast) {
        return DataTypeCasts.supportsCast(sourceType, targetType, allowExplicitCast);
    }

    private static DataField assignIdForNewField(DataField field, AtomicInteger highestFieldId) {
        DataType dataType = ReassignFieldId.reassign(field.type(), highestFieldId);
        return new DataField(
                highestFieldId.incrementAndGet(),
                field.name(),
                dataType,
                field.description(),
                field.defaultValue());
    }
}
