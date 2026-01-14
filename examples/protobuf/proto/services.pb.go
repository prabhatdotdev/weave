// Code manually written to provide Protocol Buffer types for the example.
// It mirrors the generated code that `protoc` would create.

package proto

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"

	proto "google.golang.org/protobuf/proto"
	descpb "google.golang.org/protobuf/types/descriptorpb"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Profile represents a user's profile information.
type Profile struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UserId   string `protobuf:"bytes,1,opt,name=user_id,json=userId,proto3" json:"user_id,omitempty"`
	Bio      string `protobuf:"bytes,2,opt,name=bio,proto3" json:"bio,omitempty"`
	Avatar   string `protobuf:"bytes,3,opt,name=avatar,proto3" json:"avatar,omitempty"`
	Location string `protobuf:"bytes,4,opt,name=location,proto3" json:"location,omitempty"`
	Website  string `protobuf:"bytes,5,opt,name=website,proto3" json:"website,omitempty"`
	Verified bool   `protobuf:"varint,6,opt,name=verified,proto3" json:"verified,omitempty"`
}

func (x *Profile) Reset() {
	*x = Profile{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Profile) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Profile) ProtoMessage() {}

func (x *Profile) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Profile) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{0}
}

func (x *Profile) GetUserId() string {
	if x != nil {
		return x.UserId
	}
	return ""
}

func (x *Profile) GetBio() string {
	if x != nil {
		return x.Bio
	}
	return ""
}

func (x *Profile) GetAvatar() string {
	if x != nil {
		return x.Avatar
	}
	return ""
}

func (x *Profile) GetLocation() string {
	if x != nil {
		return x.Location
	}
	return ""
}

func (x *Profile) GetWebsite() string {
	if x != nil {
		return x.Website
	}
	return ""
}

func (x *Profile) GetVerified() bool {
	if x != nil {
		return x.Verified
	}
	return false
}

// GetProfileRequest is the request to get a profile by user ID.
type GetProfileRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UserId string `protobuf:"bytes,1,opt,name=user_id,json=userId,proto3" json:"user_id,omitempty"`
}

func (x *GetProfileRequest) Reset() {
	*x = GetProfileRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetProfileRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetProfileRequest) ProtoMessage() {}

func (x *GetProfileRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetProfileRequest) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{1}
}

func (x *GetProfileRequest) GetUserId() string {
	if x != nil {
		return x.UserId
	}
	return ""
}

// GetProfileResponse is the response containing a profile.
type GetProfileResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Profile *Profile `protobuf:"bytes,1,opt,name=profile,proto3" json:"profile,omitempty"`
	Error   string   `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *GetProfileResponse) Reset() {
	*x = GetProfileResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetProfileResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetProfileResponse) ProtoMessage() {}

func (x *GetProfileResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetProfileResponse) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{2}
}

func (x *GetProfileResponse) GetProfile() *Profile {
	if x != nil {
		return x.Profile
	}
	return nil
}

func (x *GetProfileResponse) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

// ListProfilesRequest is the request to list all profiles.
type ListProfilesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ListProfilesRequest) Reset() {
	*x = ListProfilesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListProfilesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListProfilesRequest) ProtoMessage() {}

func (x *ListProfilesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ListProfilesRequest) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{3}
}

// ListProfilesResponse is the response containing all profiles.
type ListProfilesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Profiles []*Profile `protobuf:"bytes,1,rep,name=profiles,proto3" json:"profiles,omitempty"`
}

func (x *ListProfilesResponse) Reset() {
	*x = ListProfilesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListProfilesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListProfilesResponse) ProtoMessage() {}

func (x *ListProfilesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ListProfilesResponse) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{4}
}

func (x *ListProfilesResponse) GetProfiles() []*Profile {
	if x != nil {
		return x.Profiles
	}
	return nil
}

// User represents a user in the system.
type User struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id    string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name  string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Email string `protobuf:"bytes,3,opt,name=email,proto3" json:"email,omitempty"`
}

func (x *User) Reset() {
	*x = User{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *User) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*User) ProtoMessage() {}

func (x *User) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*User) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{5}
}

func (x *User) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *User) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *User) GetEmail() string {
	if x != nil {
		return x.Email
	}
	return ""
}

// EnrichedUser combines user data with profile data.
type EnrichedUser struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id      string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name    string   `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Email   string   `protobuf:"bytes,3,opt,name=email,proto3" json:"email,omitempty"`
	Profile *Profile `protobuf:"bytes,4,opt,name=profile,proto3" json:"profile,omitempty"`
}

func (x *EnrichedUser) Reset() {
	*x = EnrichedUser{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EnrichedUser) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EnrichedUser) ProtoMessage() {}

func (x *EnrichedUser) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*EnrichedUser) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{6}
}

func (x *EnrichedUser) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *EnrichedUser) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *EnrichedUser) GetEmail() string {
	if x != nil {
		return x.Email
	}
	return ""
}

func (x *EnrichedUser) GetProfile() *Profile {
	if x != nil {
		return x.Profile
	}
	return nil
}

// GetUserRequest is the request to get a user by ID.
type GetUserRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *GetUserRequest) Reset() {
	*x = GetUserRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetUserRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetUserRequest) ProtoMessage() {}

func (x *GetUserRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetUserRequest) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{7}
}

func (x *GetUserRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

// GetUserResponse is the response containing a user.
type GetUserResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	User  *EnrichedUser `protobuf:"bytes,1,opt,name=user,proto3" json:"user,omitempty"`
	Error string        `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *GetUserResponse) Reset() {
	*x = GetUserResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetUserResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetUserResponse) ProtoMessage() {}

func (x *GetUserResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetUserResponse) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{8}
}

func (x *GetUserResponse) GetUser() *EnrichedUser {
	if x != nil {
		return x.User
	}
	return nil
}

func (x *GetUserResponse) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

// ListUsersRequest is the request to list all users.
type ListUsersRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ListUsersRequest) Reset() {
	*x = ListUsersRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListUsersRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListUsersRequest) ProtoMessage() {}

func (x *ListUsersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ListUsersRequest) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{9}
}

// ListUsersResponse is the response containing all users.
type ListUsersResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Users []*EnrichedUser `protobuf:"bytes,1,rep,name=users,proto3" json:"users,omitempty"`
}

func (x *ListUsersResponse) Reset() {
	*x = ListUsersResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListUsersResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListUsersResponse) ProtoMessage() {}

func (x *ListUsersResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ListUsersResponse) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{10}
}

func (x *ListUsersResponse) GetUsers() []*EnrichedUser {
	if x != nil {
		return x.Users
	}
	return nil
}

// UserEvent represents an event related to user activity.
type UserEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EventType string `protobuf:"bytes,1,opt,name=event_type,json=eventType,proto3" json:"event_type,omitempty"`
	UserId    string `protobuf:"bytes,2,opt,name=user_id,json=userId,proto3" json:"user_id,omitempty"`
	Timestamp int64  `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *UserEvent) Reset() {
	*x = UserEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_services_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UserEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UserEvent) ProtoMessage() {}

func (x *UserEvent) ProtoReflect() protoreflect.Message {
	mi := &file_proto_services_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*UserEvent) Descriptor() ([]byte, []int) {
	return file_proto_services_proto_rawDescGZIP(), []int{11}
}

func (x *UserEvent) GetEventType() string {
	if x != nil {
		return x.EventType
	}
	return ""
}

func (x *UserEvent) GetUserId() string {
	if x != nil {
		return x.UserId
	}
	return ""
}

func (x *UserEvent) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

var File_proto_services_proto protoreflect.FileDescriptor

var file_proto_services_proto_rawDesc = buildFileDescriptor()
var file_proto_services_proto_once sync.Once
var file_proto_services_proto_rawDescData = file_proto_services_proto_rawDesc

func file_proto_services_proto_rawDescGZIP() []byte {
	file_proto_services_proto_once.Do(func() {
		file_proto_services_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_services_proto_rawDescData)
	})
	return file_proto_services_proto_rawDescData
}

var file_proto_services_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_proto_services_proto_goTypes = []interface{}{
	(*Profile)(nil),
	(*GetProfileRequest)(nil),
	(*GetProfileResponse)(nil),
	(*ListProfilesRequest)(nil),
	(*ListProfilesResponse)(nil),
	(*User)(nil),
	(*EnrichedUser)(nil),
	(*GetUserRequest)(nil),
	(*GetUserResponse)(nil),
	(*ListUsersRequest)(nil),
	(*ListUsersResponse)(nil),
	(*UserEvent)(nil),
}
var file_proto_services_proto_depIdxs = []int32{
	0, // GetProfileResponse.profile:type_name -> examples.Profile
	0, // ListProfilesResponse.profiles:type_name -> examples.Profile
	0, // EnrichedUser.profile:type_name -> examples.Profile
	6, // GetUserResponse.user:type_name -> examples.EnrichedUser
	6, // ListUsersResponse.users:type_name -> examples.EnrichedUser
}

func buildFileDescriptor() []byte {
	file := &descpb.FileDescriptorProto{
		Name:    proto.String("proto/services.proto"),
		Package: proto.String("examples"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descpb.DescriptorProto{
			profileDescriptor(),
			getProfileRequestDescriptor(),
			getProfileResponseDescriptor(),
			listProfilesRequestDescriptor(),
			listProfilesResponseDescriptor(),
			userDescriptor(),
			enrichedUserDescriptor(),
			getUserRequestDescriptor(),
			getUserResponseDescriptor(),
			listUsersRequestDescriptor(),
			listUsersResponseDescriptor(),
			userEventDescriptor(),
		},
	}
	out, err := proto.Marshal(file)
	if err != nil {
		panic(err)
	}
	return out
}

func profileDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("Profile"),
		Field: []*descpb.FieldDescriptorProto{
			field("user_id", 1, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("bio", 2, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("avatar", 3, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("location", 4, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("website", 5, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("verified", 6, descpb.FieldDescriptorProto_TYPE_BOOL, ""),
		},
	}
}

func getProfileRequestDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("GetProfileRequest"),
		Field: []*descpb.FieldDescriptorProto{
			field("user_id", 1, descpb.FieldDescriptorProto_TYPE_STRING, ""),
		},
	}
}

func getProfileResponseDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("GetProfileResponse"),
		Field: []*descpb.FieldDescriptorProto{
			field("profile", 1, descpb.FieldDescriptorProto_TYPE_MESSAGE, ".examples.Profile"),
			field("error", 2, descpb.FieldDescriptorProto_TYPE_STRING, ""),
		},
	}
}

func listProfilesRequestDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("ListProfilesRequest"),
	}
}

func listProfilesResponseDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("ListProfilesResponse"),
		Field: []*descpb.FieldDescriptorProto{
			field("profiles", 1, descpb.FieldDescriptorProto_TYPE_MESSAGE, ".examples.Profile").WithLabel(descpb.FieldDescriptorProto_LABEL_REPEATED),
		},
	}
}

func userDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("User"),
		Field: []*descpb.FieldDescriptorProto{
			field("id", 1, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("name", 2, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("email", 3, descpb.FieldDescriptorProto_TYPE_STRING, ""),
		},
	}
}

func enrichedUserDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("EnrichedUser"),
		Field: []*descpb.FieldDescriptorProto{
			field("id", 1, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("name", 2, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("email", 3, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("profile", 4, descpb.FieldDescriptorProto_TYPE_MESSAGE, ".examples.Profile"),
		},
	}
}

func getUserRequestDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("GetUserRequest"),
		Field: []*descpb.FieldDescriptorProto{
			field("id", 1, descpb.FieldDescriptorProto_TYPE_STRING, ""),
		},
	}
}

func getUserResponseDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("GetUserResponse"),
		Field: []*descpb.FieldDescriptorProto{
			field("user", 1, descpb.FieldDescriptorProto_TYPE_MESSAGE, ".examples.EnrichedUser"),
			field("error", 2, descpb.FieldDescriptorProto_TYPE_STRING, ""),
		},
	}
}

func listUsersRequestDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("ListUsersRequest"),
	}
}

func listUsersResponseDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("ListUsersResponse"),
		Field: []*descpb.FieldDescriptorProto{
			field("users", 1, descpb.FieldDescriptorProto_TYPE_MESSAGE, ".examples.EnrichedUser").WithLabel(descpb.FieldDescriptorProto_LABEL_REPEATED),
		},
	}
}

func userEventDescriptor() *descpb.DescriptorProto {
	return &descpb.DescriptorProto{
		Name: proto.String("UserEvent"),
		Field: []*descpb.FieldDescriptorProto{
			field("event_type", 1, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("user_id", 2, descpb.FieldDescriptorProto_TYPE_STRING, ""),
			field("timestamp", 3, descpb.FieldDescriptorProto_TYPE_INT64, ""),
		},
	}
}

func field(name string, number int32, typ descpb.FieldDescriptorProto_Type, typeName string) *descpb.FieldDescriptorProto {
	f := &descpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Label:  descpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:   typ.Enum(),
	}
	if typeName != "" {
		f.TypeName = proto.String(typeName)
	}
	return f
}

func init() {
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_services_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_services_proto_goTypes,
		DependencyIndexes: file_proto_services_proto_depIdxs,
		MessageInfos:      file_proto_services_proto_msgTypes,
	}.Build()
	File_proto_services_proto = out.File
	file_proto_services_proto_rawDesc = nil
	file_proto_services_proto_goTypes = nil
	file_proto_services_proto_depIdxs = nil
	for i := range file_proto_services_proto_msgTypes {
		file_proto_services_proto_msgTypes[i].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*protoimpl.MessageState); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
}
