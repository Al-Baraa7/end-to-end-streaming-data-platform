# from copy import deepcopy


# class MongoDirtyDataInjector:

#     def inject_videos(self, videos):
#         videos = deepcopy(videos)

#         if videos:
#             videos[0]["title"] = None

#         if len(videos) > 1:
#             videos[1]["duration_sec"] = 0

#         if len(videos) > 2:
#             videos[2]["duration_sec"] = 20000

#         if len(videos) > 3:
#             videos[3]["content_type"] = "live"
#             videos[3]["match_id"] = None

#         if len(videos) > 4:
#             videos[4]["content_type"] = "live"
#             videos[4]["status"] = "ended"

#         if len(videos) > 5:
#             videos[5]["stats"]["views"] = 100
#             videos[5]["stats"]["likes"] = 150

#         if len(videos) > 6:
#             videos[6]["competition"]["id"] = "RSL"

#             if videos[6]["tags"] is None:
#                 videos[6]["tags"] = []

#             videos[6]["tags"].append("Kings_Cup")

#         return videos

#     def inject(self, *, videos):
#         return self.inject_videos(videos)

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