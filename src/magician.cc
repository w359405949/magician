#include "magician.h"

Magician::Magician(uv_loop_t* loop)
    :id_string_("id"),
    loop_(loop),
{
}

google::protobuf::Message* MutableModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth=0)
{
    if (depth < 0) {
        return NULL;
    }

    std::string key = descriptor->full_name() + "_" + id;

    // level 1
    google::protobuf::Message* model = cache_rw_[key];
    if (model == NULL) {
        if (cache_ro_.find(key) != cache_ro_.end()) { // level 2
            model = cache_ro_[key]; // totally equal with db, no alter
            cache_ro_.erase(key);
        } else {
            // level 3
            model = GenerateModel(key);
        }
    }
    if (model == NULL) {
        model = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor)->New();
        cache_rw_[key] = model;
        return model;
    }
    cache_rw_[key] = model;

    for (int i = 0; i < descriptor->field_count(); i++) {
        const google::protobuf::FieldDescriptor* field_descriptor = descriptor->field(i);
        const google::protobuf::FieldDescriptor* sub_model_id_descriptor = field_descriptor->containing_type()->FindFieldByName(id_string_);
        if (sub_model_id_descriptor == NULL) {
            continue;
        }

        const google::protobuf::Message& sub_model = model->GetMetadata().reflection.GetMessage(model, field_descriptor);
        const std::string& sub_model_id = sub_model.GetMetadata().reflection.GetStringReference(sub_model, sub_model_id_descriptor);
        if (sub_model_id == ::google::protobuf::internal::GetEmptyString()) {
            continue;
        }

        google::protobuf::Message* real_sub_model = MutableModel(sub_model_id_descriptor->message_type(), sub_model_id, depth - 1);
        google::protobuf::Message* orig_sub_model = model.GetMetadata().reflection.ReleaseMessage(model, field_descriptor);
        if (orig_sub_model != NULL) {
            std::string ref_key = key + "_" + sub_model_id_descriptor.full_name() + "_" + field_id;
            cache_orig_[ref_key] = orig_sub_model;
        }

        field.GetMetadata().reflection.SetAllocatedMessage(model, real_sub_model, field_descriptor);
    }

    return model;
}

void Magician::OnAutoSave(uv_idle_t* handle)
{
    Magician* self = (magician*)handle->data;
    self->SaveAll();
}

void Magician::SaveAll()
{
    Save(cache_atomic_);
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

google::protobuf::Message* LevelDBImpl::GenerateModel(const google::protobuf::Descriptor* descriptor, const std::string& key)
{
    const google::protobuf::Message* prototype = NULL;
    prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    google::protobuf::Message* model = NULL;

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
