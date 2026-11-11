package common

// AppScoped is the kafka topic or consumer group name of the app on the profile, the PROFILE a service reads when it
// starts. every app runs its own chat, messagepreprocess, messagepersist and messagerelay on the one kafka cluster, so
// a name is the app's alone, like "holidayfriend.chat-message". production keeps the plain name: its topics were there
// before the others
func AppScoped(profile, name string) string {
	if profile == "" || profile == "production" {
		return name
	}
	return profile + "." + name
}
