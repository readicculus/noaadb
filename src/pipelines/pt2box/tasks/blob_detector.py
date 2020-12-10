import cv2

params = cv2.SimpleBlobDetector_Params()

# Change thresholds
params.minThreshold = 0
params.maxThreshold = 190

# Filter by Area.
params.filterByArea = True
params.minArea = 10
params.maxArea = 50

# Filter by Circularity
params.filterByCircularity = False
params.minCircularity = 0.1

# Filter by Convexity
params.filterByConvexity = True
params.minConvexity = 0.9
# params.maxConvexity = .99

# Filter by Inertia
params.filterByInertia = False
params.minInertiaRatio = 0.0
params.maxInertiaRatio = 0.9

# color filter seems to not work
# params.filterByColor = True
# params.blobColor = 1

params.minDistBetweenBlobs = 1

# create detector
detector = cv2.SimpleBlobDetector_create(params)


def detect(im):
    keypoints = detector.detect(cv2.bitwise_not(im))
    detections = []
    for k in keypoints:
        dim = int(k.size / 2)
        det = {'x1': k.pt[0] - dim, 'y1':k.pt[1] - dim, 'x2':k.pt[0] + dim, 'y2':k.pt[1] + dim}
        detections.append(det)
    return detections
