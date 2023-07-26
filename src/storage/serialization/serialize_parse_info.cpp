//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

void ParseInfo::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty("info_type", info_type);
}

unique_ptr<ParseInfo> ParseInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto info_type = deserializer.ReadProperty<ParseInfoType>("info_type");
	unique_ptr<ParseInfo> result;
	switch (info_type) {
	case ParseInfoType::ALTER_INFO:
		result = AlterInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::ATTACH_INFO:
		result = AttachInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::COPY_INFO:
		result = CopyInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::DETACH_INFO:
		result = DetachInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::DROP_INFO:
		result = DropInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::LOAD_INFO:
		result = LoadInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::PRAGMA_INFO:
		result = PragmaInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::TRANSACTION_INFO:
		result = TransactionInfo::FormatDeserialize(deserializer);
		break;
	case ParseInfoType::VACUUM_INFO:
		result = VacuumInfo::FormatDeserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of ParseInfo!");
	}
	return result;
}

void AlterInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("type", type);
	serializer.WriteProperty("catalog", catalog);
	serializer.WriteProperty("schema", schema);
	serializer.WriteProperty("name", name);
	serializer.WriteProperty("if_not_found", if_not_found);
	serializer.WriteProperty("allow_internal", allow_internal);
}

unique_ptr<ParseInfo> AlterInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto type = deserializer.ReadProperty<AlterType>("type");
	auto catalog = deserializer.ReadProperty<string>("catalog");
	auto schema = deserializer.ReadProperty<string>("schema");
	auto name = deserializer.ReadProperty<string>("name");
	auto if_not_found = deserializer.ReadProperty<OnEntryNotFound>("if_not_found");
	auto allow_internal = deserializer.ReadProperty<bool>("allow_internal");
	unique_ptr<AlterInfo> result;
	switch (type) {
	case AlterType::ALTER_TABLE:
		result = AlterTableInfo::FormatDeserialize(deserializer);
		break;
	case AlterType::ALTER_VIEW:
		result = AlterViewInfo::FormatDeserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of AlterInfo!");
	}
	result->catalog = std::move(catalog);
	result->schema = std::move(schema);
	result->name = std::move(name);
	result->if_not_found = if_not_found;
	result->allow_internal = allow_internal;
	return std::move(result);
}

void AlterTableInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterInfo::FormatSerialize(serializer);
	serializer.WriteProperty("alter_table_type", alter_table_type);
}

unique_ptr<AlterInfo> AlterTableInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto alter_table_type = deserializer.ReadProperty<AlterTableType>("alter_table_type");
	unique_ptr<AlterTableInfo> result;
	switch (alter_table_type) {
	case AlterTableType::ADD_COLUMN:
		result = AddColumnInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::ALTER_COLUMN_TYPE:
		result = ChangeColumnTypeInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::DROP_NOT_NULL:
		result = DropNotNullInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::FOREIGN_KEY_CONSTRAINT:
		result = AlterForeignKeyInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::REMOVE_COLUMN:
		result = RemoveColumnInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::RENAME_COLUMN:
		result = RenameColumnInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::RENAME_TABLE:
		result = RenameTableInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::SET_DEFAULT:
		result = SetDefaultInfo::FormatDeserialize(deserializer);
		break;
	case AlterTableType::SET_NOT_NULL:
		result = SetNotNullInfo::FormatDeserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of AlterTableInfo!");
	}
	return std::move(result);
}

void AlterViewInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterInfo::FormatSerialize(serializer);
	serializer.WriteProperty("alter_view_type", alter_view_type);
}

unique_ptr<AlterInfo> AlterViewInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto alter_view_type = deserializer.ReadProperty<AlterViewType>("alter_view_type");
	unique_ptr<AlterViewInfo> result;
	switch (alter_view_type) {
	case AlterViewType::RENAME_VIEW:
		result = RenameViewInfo::FormatDeserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of AlterViewInfo!");
	}
	return std::move(result);
}

void AddColumnInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("new_column", new_column);
	serializer.WriteProperty("if_column_not_exists", if_column_not_exists);
}

unique_ptr<AlterTableInfo> AddColumnInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto new_column = deserializer.ReadProperty<ColumnDefinition>("new_column");
	auto result = duckdb::unique_ptr<AddColumnInfo>(new AddColumnInfo(std::move(new_column)));
	deserializer.ReadProperty("if_column_not_exists", result->if_column_not_exists);
	return std::move(result);
}

void AlterForeignKeyInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("fk_table", fk_table);
	serializer.WriteProperty("pk_columns", pk_columns);
	serializer.WriteProperty("fk_columns", fk_columns);
	serializer.WriteProperty("pk_keys", pk_keys);
	serializer.WriteProperty("fk_keys", fk_keys);
	serializer.WriteProperty("type", type);
}

unique_ptr<AlterTableInfo> AlterForeignKeyInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<AlterForeignKeyInfo>(new AlterForeignKeyInfo());
	deserializer.ReadProperty("fk_table", result->fk_table);
	deserializer.ReadProperty("pk_columns", result->pk_columns);
	deserializer.ReadProperty("fk_columns", result->fk_columns);
	deserializer.ReadProperty("pk_keys", result->pk_keys);
	deserializer.ReadProperty("fk_keys", result->fk_keys);
	deserializer.ReadProperty("type", result->type);
	return std::move(result);
}

void AttachInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("name", name);
	serializer.WriteProperty("path", path);
	serializer.WriteProperty("options", options);
}

unique_ptr<ParseInfo> AttachInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<AttachInfo>(new AttachInfo());
	deserializer.ReadProperty("name", result->name);
	deserializer.ReadProperty("path", result->path);
	deserializer.ReadProperty("options", result->options);
	return std::move(result);
}

void ChangeColumnTypeInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("column_name", column_name);
	serializer.WriteProperty("target_type", target_type);
	serializer.WriteProperty("expression", *expression);
}

unique_ptr<AlterTableInfo> ChangeColumnTypeInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<ChangeColumnTypeInfo>(new ChangeColumnTypeInfo());
	deserializer.ReadProperty("column_name", result->column_name);
	deserializer.ReadProperty("target_type", result->target_type);
	deserializer.ReadProperty("expression", result->expression);
	return std::move(result);
}

void CopyInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("catalog", catalog);
	serializer.WriteProperty("schema", schema);
	serializer.WriteProperty("table", table);
	serializer.WriteProperty("select_list", select_list);
	serializer.WriteProperty("is_from", is_from);
	serializer.WriteProperty("format", format);
	serializer.WriteProperty("file_path", file_path);
	serializer.WriteProperty("options", options);
}

unique_ptr<ParseInfo> CopyInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<CopyInfo>(new CopyInfo());
	deserializer.ReadProperty("catalog", result->catalog);
	deserializer.ReadProperty("schema", result->schema);
	deserializer.ReadProperty("table", result->table);
	deserializer.ReadProperty("select_list", result->select_list);
	deserializer.ReadProperty("is_from", result->is_from);
	deserializer.ReadProperty("format", result->format);
	deserializer.ReadProperty("file_path", result->file_path);
	deserializer.ReadProperty("options", result->options);
	return std::move(result);
}

void DetachInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("name", name);
	serializer.WriteProperty("if_not_found", if_not_found);
}

unique_ptr<ParseInfo> DetachInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<DetachInfo>(new DetachInfo());
	deserializer.ReadProperty("name", result->name);
	deserializer.ReadProperty("if_not_found", result->if_not_found);
	return std::move(result);
}

void DropInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("type", type);
	serializer.WriteProperty("catalog", catalog);
	serializer.WriteProperty("schema", schema);
	serializer.WriteProperty("name", name);
	serializer.WriteProperty("if_not_found", if_not_found);
	serializer.WriteProperty("cascade", cascade);
	serializer.WriteProperty("allow_drop_internal", allow_drop_internal);
}

unique_ptr<ParseInfo> DropInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<DropInfo>(new DropInfo());
	deserializer.ReadProperty("type", result->type);
	deserializer.ReadProperty("catalog", result->catalog);
	deserializer.ReadProperty("schema", result->schema);
	deserializer.ReadProperty("name", result->name);
	deserializer.ReadProperty("if_not_found", result->if_not_found);
	deserializer.ReadProperty("cascade", result->cascade);
	deserializer.ReadProperty("allow_drop_internal", result->allow_drop_internal);
	return std::move(result);
}

void DropNotNullInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("column_name", column_name);
}

unique_ptr<AlterTableInfo> DropNotNullInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<DropNotNullInfo>(new DropNotNullInfo());
	deserializer.ReadProperty("column_name", result->column_name);
	return std::move(result);
}

void LoadInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("filename", filename);
	serializer.WriteProperty("load_type", load_type);
}

unique_ptr<ParseInfo> LoadInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<LoadInfo>(new LoadInfo());
	deserializer.ReadProperty("filename", result->filename);
	deserializer.ReadProperty("load_type", result->load_type);
	return std::move(result);
}

void PragmaInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("name", name);
	serializer.WriteProperty("parameters", parameters);
	serializer.WriteProperty("named_parameters", named_parameters);
}

unique_ptr<ParseInfo> PragmaInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<PragmaInfo>(new PragmaInfo());
	deserializer.ReadProperty("name", result->name);
	deserializer.ReadProperty("parameters", result->parameters);
	deserializer.ReadProperty("named_parameters", result->named_parameters);
	return std::move(result);
}

void RemoveColumnInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("removed_column", removed_column);
	serializer.WriteProperty("if_column_exists", if_column_exists);
	serializer.WriteProperty("cascade", cascade);
}

unique_ptr<AlterTableInfo> RemoveColumnInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<RemoveColumnInfo>(new RemoveColumnInfo());
	deserializer.ReadProperty("removed_column", result->removed_column);
	deserializer.ReadProperty("if_column_exists", result->if_column_exists);
	deserializer.ReadProperty("cascade", result->cascade);
	return std::move(result);
}

void RenameColumnInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("old_name", old_name);
	serializer.WriteProperty("new_name", new_name);
}

unique_ptr<AlterTableInfo> RenameColumnInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<RenameColumnInfo>(new RenameColumnInfo());
	deserializer.ReadProperty("old_name", result->old_name);
	deserializer.ReadProperty("new_name", result->new_name);
	return std::move(result);
}

void RenameTableInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("new_table_name", new_table_name);
}

unique_ptr<AlterTableInfo> RenameTableInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<RenameTableInfo>(new RenameTableInfo());
	deserializer.ReadProperty("new_table_name", result->new_table_name);
	return std::move(result);
}

void RenameViewInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterViewInfo::FormatSerialize(serializer);
	serializer.WriteProperty("new_view_name", new_view_name);
}

unique_ptr<AlterViewInfo> RenameViewInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<RenameViewInfo>(new RenameViewInfo());
	deserializer.ReadProperty("new_view_name", result->new_view_name);
	return std::move(result);
}

void SetDefaultInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("column_name", column_name);
	serializer.WriteOptionalProperty("expression", expression);
}

unique_ptr<AlterTableInfo> SetDefaultInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<SetDefaultInfo>(new SetDefaultInfo());
	deserializer.ReadProperty("column_name", result->column_name);
	deserializer.ReadOptionalProperty("expression", result->expression);
	return std::move(result);
}

void SetNotNullInfo::FormatSerialize(FormatSerializer &serializer) const {
	AlterTableInfo::FormatSerialize(serializer);
	serializer.WriteProperty("column_name", column_name);
}

unique_ptr<AlterTableInfo> SetNotNullInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<SetNotNullInfo>(new SetNotNullInfo());
	deserializer.ReadProperty("column_name", result->column_name);
	return std::move(result);
}

void TransactionInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("type", type);
}

unique_ptr<ParseInfo> TransactionInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = duckdb::unique_ptr<TransactionInfo>(new TransactionInfo());
	deserializer.ReadProperty("type", result->type);
	return std::move(result);
}

void VacuumInfo::FormatSerialize(FormatSerializer &serializer) const {
	ParseInfo::FormatSerialize(serializer);
	serializer.WriteProperty("options", options);
}

unique_ptr<ParseInfo> VacuumInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto options = deserializer.ReadProperty<VacuumOptions>("options");
	auto result = duckdb::unique_ptr<VacuumInfo>(new VacuumInfo(options));
	return std::move(result);
}

} // namespace duckdb
