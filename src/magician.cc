#include "magician.h"

Magician::Magician(uv_loop_t* loop)
    :id_string_("id"),
    loop_(loop),
{
}

google::protobuf::Message* MutableModel(const ::google::protobuf::Descriptor* descriptor, const std::string& id, int depth)
{
    std::string key = descriptor->full_name() + "_" + id;

    ::google::protobuf::Message* model = NULL;

    if (cache_node_rw_.find(key) != cache_node_rw_.end()) {
        model = cache_node_rw_[key];
        if (depth > 1) {
            FillMutableModel(model, depth);
        }
    } else if (cache_node_ro_.find(key) != cache_node_ro_.end()) {
        cache_node_rw_[key] = cache_node_ro_[key];
        cache_node_ro_.erase(key);
        model = cache_node_rw_[key];
        if (depth > 1) {
            FillMutableModel(model, depth);
        }
    } else if (cache_leaf_rw_.find(key) != cache_leaf_rw_.end()) {
        model = cache_leaf_rw_[key];
        if (depth > 0) {
            FillMutableModel(model, depth);
            cache_node_rw_[key] = model;
            cache_leaf_rw_.erase(key);
        }
    } else if (cache_leaf_ro_.find(key) != cache_leaf_ro_.end()) {
        model = cache_leaf_ro_[key];
        cache_leaf_ro_.erase(key);
        if (depth > 0) {
            FillMutableModel(model, depth);
            cache_node_rw_[key] = model;
        } else {
            cache_leaf_rw_[key] = model;
        }
    } else {
        model = GenerateModel(key);
        if (model == NULL) {
            model = ::google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor)->New();
        }
        if (depth > 0) {
            FillMutableModel(model, depth);
            cache_node_rw_[key] = model;
        } else {
            cache_leaf_rw_[key] = model;
        }
    }

    return model;
}

void Magician::FillMutableModel(google::protobuf::Message* model)
{
    ::google::protobuf::Descriptor* descriptor = message->descriptor();

    for (int i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::FieldDescriptor* field_descriptor = descriptor->field(i);
        const ::google::protobuf::FieldDescriptor* sub_model_id_descriptor = field_descriptor->containing_type()->FindFieldByName(id_string_);
        if (sub_model_id_descriptor == NULL) {
            continue;
        }

        const ::google::protobuf::Message& sub_model = model->GetMetadata().reflection.GetMessage(model, field_descriptor);
        const std::string& sub_model_id = sub_model.GetMetadata().reflection.GetStringReference(sub_model, sub_model_id_descriptor);
        if (sub_model_id == ::google::protobuf::internal::GetEmptyString()) {
            continue;
        }

        ::google::protobuf::Message* real_sub_model = MutableModel(sub_model_id_descriptor->message_type(), sub_model_id, depth - 1);
        ::google::protobuf::Message* orig_sub_model = model.GetMetadata().reflection.ReleaseMessage(model, field_descriptor);
        if (orig_sub_model != NULL) {
            std::string ref_key = sub_model_id_descriptor.full_name() + "_" + field_id;
            cache_orig_[ref_key] = orig_sub_model;
        }

        field.GetMetadata().reflection.SetAllocatedMessage(model, real_sub_model, field_descriptor);
    }
}

void Magician::OnAutoSave(uv_idle_t* handle)
{
    Magician* self = (magician*)handle->data;
    self->SaveAll();
}

void Magician::SaveAll()
{
    std::map<std::string, ::google::protobuf::Message*>::iterator it;
    for (it = cache_node_rw_.begin(); it != cache_node_rw_.end(); ++it) {
        ::google::protobuf::Descriptor* descriptor = it->second->descriptor();
        ::google::protobuf::Message* model = it->second;
        for (int i = 0; i < descriptor->field_count(); i++) {
            const ::google::protobuf::FieldDescriptor* field_descriptor = descriptor->field(i);
            const ::google::protobuf::FieldDescriptor* sub_model_id_descriptor = field_descriptor->containing_type()->FindFieldByName(id_string_);
            if (sub_model_id_descriptor == NULL) {
                continue;
            }
            ::google::protobuf::Message* sub_model = model.GetMetadata().reflection.ReleaseMessage(model, field_descriptor);
            if (sub_model == NULL) {
                continue;
            }

            const std::string& sub_model_id = sub_model.GetMetadata().reflection.GetStringReference(sub_model, sub_model_id_descriptor);
            if (sub_model_id == ::google::protobuf::internal::GetEmptyString()) {
                continue;
            }

            std::string ref_key = sub_model_id_descriptor.full_name() + "_" + field_id;

            ::google::protobuf::Message* orig_sub_model = cache_orig_[ref_key];
            cache_orig_.erase(ref_key);
            if (orig_sub_model == NULL) {
                orig_sub_model = model.GetMetadata().reflection.MutableMessage(model, field_descriptor);
                orig_sub_model.GetMetadata().reflection.GetStringReference(sub_model, sub_model_id_descriptor);
            } else {
                field.GetMetadata().reflection.SetAllocatedMessage(model, orig_sub_model, field_descriptor);
            }
        }
    }

    bool result = Save(cache_leaf_rw_);
    if (result) {
        cache_leaf_rw_.insert(cache_atomic_rw_.begin(), cache_atomic_rw_.end());
        cache_leaf_rw_.clear();
    } else {
        std::map<std::string, ::google::protobuf::Message*>::iterator it;
        for (it = cache_leaf_ro_.begin(); it != cache_leaf_ro_.end(); it++) {
            delete it->second;
        }
        cache_leaf_ro_.clear();
    }
}

std::map<std::string, LevelDBImpl*> LevelDBImpl::dbs_;

LevelDBImpl* LevelDBImpl::Open(uv_loop_t* loop, std::string path)
{
    std::map<std::string, LevelDBImpl*>::iterator it = dbs_.find(path);
    if (it != dbs_.end()) {
        return it->second;
    }

    leveldb::DB* db = NULL;

    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, path, &db);
    if (!status.ok()) {
        return NULL;
    }

    LevelDBImpl* leveldb_impl = new LevelDBImpl(loop);
    leveldb_impl.db_ = db;

    dbs_[path] = leveldb_impl;
    return leveldb_impl;
}

void LevelDBImpl::CloseAll()
{
    std::map<std::string, LevelDBImpl*>::iterator it;
    for (it = dbs_.begin(); it != dbs_.end(); it++) {
        if (it->second != NULL) {
            delete it->second;
        }
    }

    dbs_.clear();
}

google::protobuf::Message* LevelDBImpl::GenerateModel(const ::google::protobuf::Descriptor* descriptor, const std::string& key)
{
    const ::google::protobuf::Message* prototype = NULL;
    prototype = ::google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    ::google::protobuf::Message* model = NULL;

    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
        model = prototype->New();
        model->ParseFromString(value);
    }
    return model;
}

bool LevelDBImpl::Save(std::map<std::string, ::google::protobuf::Message*> models)
{
    std::map<std::string, ::google::protobuf::Message*>::iterator it;

    leveldb::WriteBatch batch;
    for (it = models.begin(); it != models.end(); it++) {
        std::string value;
        ::google::protobuf::Message* model = it->second;
        CHECK(model);
        if (!model->SerializeToString(&value)) {
            continue;
        }
        batch.Put(it->first, value);
    }

    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    return status.ok();
}
