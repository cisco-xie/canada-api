package com.example.demo.common.utils;

import lombok.extern.slf4j.Slf4j;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;

@Slf4j
public class CaptchaSolver {

    private static final int TEMPLATE_WIDTH = 30;
    private static final int TEMPLATE_HEIGHT = 40;

    // 模板缓存: 0-9 -> List<BufferedImage>
    private static final Map<Integer, List<BufferedImage>> templates = new HashMap<>();

    static {
        try {
            for (int i = 0; i <= 9; i++) {
                List<BufferedImage> list = new ArrayList<>();
                File dir = new File(CaptchaSolver.class.getClassLoader()
                        .getResource("captcha_templates/" + i).toURI());
                for (File f : Objects.requireNonNull(dir.listFiles((d, name) -> name.endsWith(".png")))) {
                    BufferedImage img = ImageIO.read(f);
                    list.add(img);
                }
                templates.put(i, list);
                log.info("加载模板 {} -> {} 张", i, list.size());
            }
        } catch (Exception e) {
            log.error("加载验证码模板失败", e);
        }
    }

    /** 识别验证码 */
    public static String solveCaptcha(byte[] imageBytes) throws IOException {
        BufferedImage captchaImage = ImageIO.read(new java.io.ByteArrayInputStream(imageBytes));

        // 灰度化和二值化
        BufferedImage binary = toBinary(toGray(captchaImage));

        // 切割数字
        int[][] bounds = getDigitBounds(binary);
        StringBuilder code = new StringBuilder();
        for (int[] bound : bounds) {
            BufferedImage digitImg = binary.getSubimage(bound[0], 0, bound[1] - bound[0], binary.getHeight());
            BufferedImage resized = resize(digitImg, TEMPLATE_WIDTH, TEMPLATE_HEIGHT);
            int matched = matchDigit(resized);
            code.append(matched);
        }
        return code.toString();
    }

    private static BufferedImage toGray(BufferedImage src) {
        BufferedImage gray = new BufferedImage(src.getWidth(), src.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
        Graphics g = gray.getGraphics();
        g.drawImage(src, 0, 0, null);
        g.dispose();
        return gray;
    }

    private static BufferedImage toBinary(BufferedImage gray) {
        BufferedImage binary = new BufferedImage(gray.getWidth(), gray.getHeight(), BufferedImage.TYPE_BYTE_BINARY);
        Graphics g = binary.getGraphics();
        g.drawImage(gray, 0, 0, null);
        g.dispose();
        return binary;
    }

    private static BufferedImage resize(BufferedImage src, int width, int height) {
        BufferedImage resized = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_BINARY);
        Graphics2D g2 = resized.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g2.drawImage(src, 0, 0, width, height, null);
        g2.dispose();
        return resized;
    }

    private static int[][] getDigitBounds(BufferedImage binary) {
        int w = binary.getWidth();
        int h = binary.getHeight();
        List<int[]> list = new ArrayList<>();
        boolean inDigit = false;
        int start = 0;
        for (int x = 0; x < w; x++) {
            int blackCount = 0;
            for (int y = 0; y < h; y++) {
                int rgb = binary.getRGB(x, y) & 0xFF;
                if (rgb == 0) blackCount++;
            }
            if (!inDigit && blackCount > h * 0.05) {
                start = x;
                inDigit = true;
            } else if (inDigit && blackCount <= h * 0.05) {
                list.add(new int[]{start, x});
                inDigit = false;
            }
        }
        if (inDigit) list.add(new int[]{start, w});
        return list.toArray(new int[0][]);
    }

    /** 匹配数字模板 */
    private static int matchDigit(BufferedImage digitImg) {
        int bestDigit = 0;
        double bestScore = Double.MAX_VALUE;
        for (int i = 0; i <= 9; i++) {
            for (BufferedImage tmpl : templates.get(i)) {
                double score = compareImages(digitImg, tmpl);
                if (score < bestScore) {
                    bestScore = score;
                    bestDigit = i;
                }
            }
        }
        return bestDigit;
    }

    /** 简单像素差异平方和 */
    private static double compareImages(BufferedImage img1, BufferedImage img2) {
        int w = Math.min(img1.getWidth(), img2.getWidth());
        int h = Math.min(img1.getHeight(), img2.getHeight());
        double sum = 0;
        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                int rgb1 = img1.getRGB(x, y) & 0xFF;
                int rgb2 = img2.getRGB(x, y) & 0xFF;
                double diff = rgb1 - rgb2;
                sum += diff * diff;
            }
        }
        return sum;
    }
}
