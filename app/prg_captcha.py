import cv2
import torch
import numpy as np
import torch.nn as nn
import torch.nn.functional as F


class PixelCaptchaCNN(nn.Module):
    def __init__(self, num_classes, captcha_len):
        super().__init__()

        self.features = nn.Sequential(
            nn.Conv2d(1, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d((2, 2)),

            nn.Conv2d(32, 64, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d((2, 1)),

            nn.Conv2d(64, 128, 3, padding=1),
            nn.ReLU(),
        )

        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(128 * 10 * 60, 512),
            nn.ReLU(),
            nn.Linear(512, captcha_len * num_classes)
        )

        self.captcha_len = captcha_len
        self.num_classes = num_classes

    def forward(self, x):
        x = self.features(x)
        x = self.classifier(x)
        x = x.view(-1, self.captcha_len, self.num_classes)
        return x


#guess captcha
class CaptchaSolver:
    def __init__(self, model_path, device=None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")

        checkpoint = torch.load(model_path, map_location=self.device)

        self.model = PixelCaptchaCNN(
            num_classes=checkpoint["num_classes"],
            captcha_len=checkpoint["captcha_len"]
        ).to(self.device)

        self.model.load_state_dict(checkpoint["model_state_dict"])
        self.model.eval()

        # rebuild charset
        chars = checkpoint["chars"]
        self.idx2char = {i: c for i, c in enumerate(chars)}

        self.img_width = 120
        self.img_height = 40


    def _preprocess(self, image_path):
        image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)

        if image is None:
            raise ValueError(f"Cannot read image: {image_path}")

        image = cv2.resize(
            image,
            (self.img_width, self.img_height),
            interpolation=cv2.INTER_NEAREST
        )

        image = image.astype(np.float32) / 255.0

        # IMPORTANT: threshold (same as training)
        _, image = cv2.threshold(image, 0.5, 1.0, cv2.THRESH_BINARY)

        image = np.expand_dims(image, axis=0)  # (1, H, W)
        image = np.expand_dims(image, axis=0)  # (1, 1, H, W)

        return torch.tensor(image).to(self.device)


    def predict(self, image_path):
        image = self._preprocess(image_path)

        with torch.no_grad():
            logits = self.model(image)
            preds = logits.argmax(dim=2)[0]

        return "".join(self.idx2char[i.item()] for i in preds)

    # -------------------------------
    # WITH CONFIDENCE
    # -------------------------------
    def predict_with_confidence(self, image_path):
        image = self._preprocess(image_path)

        with torch.no_grad():
            logits = self.model(image)
            probs = F.softmax(logits, dim=2)
            preds = probs.argmax(dim=2)[0]

        text = ""
        confidences = []

        for i, c in enumerate(preds):
            text += self.idx2char[c.item()]
            confidences.append(probs[0, i, c].item())

        avg_conf = float(np.mean(confidences))

        return text, confidences, avg_conf

