#include "magician.h"

Magician::Magician(uv_loop_t* loop=NULL)
    :id_string_("id"),
    loop_(loop),
    db_(NULL)
{
}

bool Magician::Open(std::string db_path)
{

}

const google::protobuf::Message& Magician::GetModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth)
{
    CHECK(descriptor->file()->pool() == DescriptorPool::generated_pool());

    const google::protobuf::Message* prototype = NULL;
    prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    google::protobuf::Message* model = NULL;
    std::string key = descriptor->full_name() + "_" + id;
    std::map<std::string, ::google::protobuf::Message*>::iterator it;

    it = cache_rw_.find(key);
    if (cache_rw_.end() != it) {
        model = it->second;
    } else {
        it = cache_ro_.find(key);
        if (cache_ro_.end() != it) {
            model = it->second;
        } else {
            model = GenerateModel(descriptor, id);
        }
    }

    for (int32_t i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::Message& field = model->GetMetadata().reflection.GetMessage(model, descriptor->field(i))
        const ::google::protobuf::FieldDescriptor* id_descriptor = field->descriptor()->FindFieldByName(id_string_);
        if (id_descriptor == NULL) {
            continue;
        }

        const std::string& field_id = field.GetMetadata().reflection.GetStringReference(field, id_descriptor);
        if (field_id == ::google::protobuf::internal::GetEmptyString()) {
            continue;
        }

        ::google::protobuf::Message* field_model = MutableModel(id_descriptor, field_id, depth - 1);

    }

    return *model;
}

google::protobuf::Message* Megician::MutableModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth, google::protobuf::Message* initial)
{
    CHECK(descriptor->file()->pool() == DescriptorPool::generated_pool());

    const google::protobuf::Message* prototype = NULL;
    prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    google::protobuf::Message* model = NULL;

    std::string key = descriptor->full_name() + "_" + id;
    std::map<std::string, ::google::protobuf::Message*>::iterator it;
    it = cache_rw_.find(key);
    if (cache_rw_.end() != it) {
        return it->second;
    }

    it = cache_ro_.find(key);
    if (cache_ro_.end() != it) {
        cache_rw_[key] = it->second;
        cache_ro_.erase(it);
        return it->second;
    }
    model = GenerateModel(descriptor, id);

    cache_rw_[key] = model;

    return model;
}

google::protobuf::Message* Megician::FillModel(google::protobuf::Message* model)
{
    for (int32_t i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::Message& field = model->GetMetadata().reflection.GetMessage(model, descriptor->field(i))
        const ::google::protobuf::FieldDescriptor* id_descriptor = field->descriptor()->FindFieldByName(id_string_);
        if (id_descriptor == NULL) {
            continue;
        }

        const std::string& field_id = field.GetMetadata().reflection.GetStringReference(field, id_descriptor);
        if (field_id == ::google::protobuf::internal::GetEmptyString()) {
            continue;
        }

        ::google::protobuf::Message* field_model = ReleaseModel(id_descriptor, field_id);
        if (field_model == NULL) {
            field_model = GenerateModel(descriptor, id);
        }
    }
}

google::protobuf::Message* Megician::ReleaseModel(const google::protobuf::Descriptor* descriptor, const std::string& id)
{
    std::string key = descriptor->full_name() + "_" + id;

    std::map<std::string, ::google::protobuf::Message*>::iterator it;
    it = cache_rw_.find(key);
    if (cache_rw_.end() != it) {
        cache_rw_.erase(it);
        return it->second;
    }

    it = cache_ro_.find(key);
    if (cache_ro_.end() != it) {
        cache_ro_.erase(it);
        return it->second;
    }

    return NULL;
}

google::protobuf::Message* Megician::GenerateModel(const google::protobuf::Descriptor* descriptor, const std::string& id)
{
    const google::protobuf::Message* prototype = NULL;
    prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    std::string key = descriptor->full_name() + "_" + id;

    cache_rw_[key] = prototype->New();
    google::protobuf::Message* model = cache_rw_[key];

    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
        model->ParseFromString(value);
    }
    return model;
}
