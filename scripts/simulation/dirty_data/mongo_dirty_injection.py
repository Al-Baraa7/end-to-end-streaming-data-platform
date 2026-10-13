from copy import deepcopy


class MongoDirtyDataInjector:

    def inject_videos(self, videos):

        for index, video in enumerate(videos):

            video = deepcopy(video)

            if index == 0:
                video["title"] = None

            elif index == 1:
                video["duration_sec"] = 0

            elif index == 2:
                video["duration_sec"] = 20000

            elif index == 3:
                video["content_type"] = "live"
                video["match_id"] = None

            elif index == 4:
                video["content_type"] = "live"
                video["status"] = "ended"

            elif index == 5:
                video["stats"]["views"] = 100
                video["stats"]["likes"] = 150

            elif index == 6:
                video["competition"]["id"] = "RSL"

                if video["tags"] is None:
                    video["tags"] = []

                video["tags"].append("Kings_Cup")

            yield video

    def inject(self, *, videos):
        return self.inject_videos(videos)