package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;

    public boolean CheckAuthoInfo(AuthInfo auth) {
        String sqlPassword = "SELECT Password FROM \"UserRecord\" WHERE mid = ?";
        String sqlQQWechatMatch = "SELECT mid FROM \"UserRecord\" WHERE qq = ? AND wechat = ?";
        String sqlQQMatch = "SELECT mid FROM \"UserRecord\" WHERE qq = ?";
        String sqlWechatMatch = "SELECT mid FROM \"UserRecord\" WHERE wechat = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtPassword = conn.prepareStatement(sqlPassword);
             PreparedStatement stmtQQWechatMatch = conn.prepareStatement(sqlQQWechatMatch);
             PreparedStatement stmtQQMatch = conn.prepareStatement(sqlQQMatch);
             PreparedStatement stmtWechatMatch = conn.prepareStatement(sqlWechatMatch);
        ) {
            if (auth.getMid() != 0 && auth.getPassword() != null) {
                stmtPassword.setLong(1, auth.getMid());
                ResultSet rsPassword = stmtPassword.executeQuery();

                if (rsPassword.next() && !auth.getPassword().equals(rsPassword.getString(1))) {
                    System.out.println("Mid and password don't match!");
                    return false;
                }
            } else if ((auth.getQq() != null && auth.getWechat() == null) || (auth.getWechat() != null && auth.getQq() == null)) {
                if (auth.getQq() != null) {
                    stmtQQMatch.setString(1, auth.getQq());
                    ResultSet rsQQMatch = stmtQQMatch.executeQuery();

                    if (rsQQMatch.next()) {
                        System.out.println("Only qq is provided!");
                        return true;
                    }
                } else if (auth.getWechat() != null) {
                    stmtWechatMatch.setString(1, auth.getWechat());
                    ResultSet rsWechatMatch = stmtWechatMatch.executeQuery();

                    if (rsWechatMatch.next()) {
                        System.out.println("Only wechat is provided!");
                        return true;
                    }
                }
            } else if (auth.getQq() != null && auth.getWechat() != null) {
                stmtQQWechatMatch.setString(1, auth.getQq());
                stmtQQWechatMatch.setString(2, auth.getWechat());
                ResultSet rsQQWechatMatch = stmtQQWechatMatch.executeQuery();

                if (rsQQWechatMatch.next()) {
                    System.out.println("QQ and Wechat match!");
                    return true;
                }
            } else {
                System.out.println("No login information is provided!");
            }

            return false;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    //如何生成独一无二的bv号？
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        String sqlSameTitleAndUser = "SELECT COUNT(*) FROM \"VideoRecord\" WHERE Title = ? AND OwnerMid = ?";
        String sqlFindMidName = "SELECT name FROM \"UserRecord\" WHERE mid = ?";
        VideoRecord newVideoRecord = new VideoRecord();

        if (!CheckAuthoInfo(auth)) {
            System.out.println("Authinfo is wrong!");
            return null;
        } else {
            if (req.getTitle() == null || req.getTitle().isEmpty()) {
                System.out.println("Title is null or empty!");
                return null;
            }

            if (req.getDuration() < 10) {
                System.out.println("Duration is too short!");
                return null;
            }

            LocalDateTime localDateTime = LocalDateTime.now();
            if (req.getPublicTime().before(Timestamp.valueOf(localDateTime))) {
                System.out.println("Public time is too early!");
                return null;
            }

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmtSameTitleAndUser = conn.prepareStatement(sqlSameTitleAndUser);
                 PreparedStatement stmtFindMidName = conn.prepareStatement(sqlFindMidName);
            ) {
                stmtSameTitleAndUser.setString(1, req.getTitle());
                stmtSameTitleAndUser.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtSameTitleAndUser);
                ResultSet rsSameTitleAndUser = stmtSameTitleAndUser.executeQuery();
                rsSameTitleAndUser.next();

                if (rsSameTitleAndUser.getInt(1) != 0) {
                    System.out.println("There is already a same video!");
                    return null;
                }

                stmtFindMidName.setLong(1, auth.getMid());
                log.info("SQL: {}", stmtFindMidName);
                ResultSet rsFindMidName = stmtFindMidName.executeQuery();
                rsFindMidName.next();
                String authName = rsFindMidName.getString(1);

                newVideoRecord.setBv(generateBv());
                newVideoRecord.setTitle(req.getTitle());
                newVideoRecord.setOwnerMid(auth.getMid());
                newVideoRecord.setOwnerName(authName);
                newVideoRecord.setCommitTime(Timestamp.valueOf(localDateTime));
                newVideoRecord.setDuration(req.getDuration());
                newVideoRecord.setDescription(req.getDescription());
                newVideoRecord.setVideo_is_Deleted(false);

                String sqlInsertVideo = "INSERT INTO \"VideoRecord\" (bv, title, ownermid, ownername, committime, duration, description) VALUES (?, ?, ?, ?, ?, ?, ?)";

                try (PreparedStatement stmtInsertVideo = conn.prepareStatement(sqlInsertVideo)) {
                    stmtInsertVideo.setString(1, newVideoRecord.getBv());
                    stmtInsertVideo.setString(2, newVideoRecord.getTitle());
                    stmtInsertVideo.setLong(3, newVideoRecord.getOwnerMid());
                    stmtInsertVideo.setString(4, newVideoRecord.getOwnerName());
                    stmtInsertVideo.setTimestamp(5, newVideoRecord.getCommitTime());
                    stmtInsertVideo.setFloat(6, newVideoRecord.getDuration());
                    stmtInsertVideo.setString(7, newVideoRecord.getDescription());
                    log.info("SQL: {}", stmtInsertVideo);
                    stmtInsertVideo.executeUpdate();
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return newVideoRecord.getBv();
    }

    public String generateBv() {
        return "BV2".concat(String.valueOf(System.currentTimeMillis()));
    }


    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        if (!CheckAuthoInfo(auth)) {
            return false;
        }

        String sqlUserIdentity = "SELECT identity FROM \"UserRecord\" WHERE mid = ?";
        String sqlVideo = "SELECT ownerMid FROM \"VideoRecord\" WHERE bv = ?";
        String sqlDeleteVideo = "DELETE FROM \"VideoRecord\" WHERE bv = ?";
        String sqlDeleteCoins = "DELETE FROM coin_user WHERE bv = ?";
        String sqlDeleteLikes = "DELETE FROM video_like WHERE bv = ?";
        String sqlDeleteCollects = "DELETE FROM video_collect WHERE bv = ?";
        String sqlDeleteDanmu = "DELETE FROM \"DanmuRecord\" where bv = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtUserIdentity = conn.prepareStatement(sqlUserIdentity);
             PreparedStatement stmtVideo = conn.prepareStatement(sqlVideo);
             PreparedStatement stmtDeleteVideo = conn.prepareStatement(sqlDeleteVideo);
             PreparedStatement stmtDeleteCoins = conn.prepareStatement(sqlDeleteCoins);
             PreparedStatement stmtDeleteLikes = conn.prepareStatement(sqlDeleteLikes);
             PreparedStatement stmtDeleteCollects = conn.prepareStatement(sqlDeleteCollects);
             PreparedStatement stmtDeleteDanmu = conn.prepareStatement(sqlDeleteDanmu);
        ) {
            stmtUserIdentity.setLong(1, auth.getMid());
            log.info("SQL: {}", stmtUserIdentity);
            ResultSet rsUserIdentity = stmtUserIdentity.executeQuery();
            rsUserIdentity.next();
            String identity = rsUserIdentity.getString(1);

            stmtVideo.setString(1, bv);
            log.info("SQL: {}", stmtVideo);
            ResultSet rsVideo = stmtVideo.executeQuery();
            Long ownerMid;

            if (!rsVideo.next()) {
                return false; // 没有找到对应的 video
            } else {
                ownerMid = rsVideo.getLong(1);
                if (!ownerMid.equals(auth.getMid()) && !identity.equals("SUPERUSER")) {
                    return false; // 不是 video 对应的 owner 且不是超级用户
                }
            }

            stmtDeleteVideo.setString(1, bv);
            log.info("SQL: {}", stmtDeleteVideo);
            stmtDeleteVideo.executeUpdate(); // 删除 VideoRecord 中的记录

            // 删除相关的项
            stmtDeleteCoins.setString(1, bv);
            log.info("SQL: {}", stmtDeleteCoins);
            stmtDeleteCoins.executeUpdate();

            stmtDeleteLikes.setString(1, bv);
            log.info("SQL: {}", stmtDeleteLikes);
            stmtDeleteLikes.executeUpdate();

            stmtDeleteCollects.setString(1, bv);
            log.info("SQL: {}", stmtDeleteCollects);
            stmtDeleteCollects.executeUpdate();

            stmtDeleteDanmu.setString(1, bv);
            log.info("SQL: {}", stmtDeleteDanmu);
            stmtDeleteDanmu.executeUpdate();

            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public Long find_VideoOwner(String bv) {
        String sqlFindOwner = "SELECT ownerMid FROM \"VideoRecord\" WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtFindOwner = conn.prepareStatement(sqlFindOwner);
        ) {
            stmtFindOwner.setString(1, bv);
            log.info("SQL: {}", stmtFindOwner);
            ResultSet rsFindOwner = stmtFindOwner.executeQuery();
            if (!rsFindOwner.next()) {
                return null; // 没有找到对应的 video
            } else {
                return rsFindOwner.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        String sqlVideoInformation = "SELECT title, description, duration, publicTime, reviewtime FROM \"VideoRecord\" WHERE bv = ?";
        String sqlUpdateVideo = "UPDATE \"VideoRecord\" SET title = ?, description = ?, publicTime = ?, reviewtime = ?,reviewer = ? WHERE bv = ?";
        if (!CheckAuthoInfo(auth)) {
            System.out.println("Authinfo is wrong!");
            return false;
        } else {
            if (req.getTitle() == null) {
                System.out.println("title is null!");
                return false;
            }
            if (req.getDuration() < 10) {
                System.out.println("duration is too short!");
                return false;
            }
            LocalDateTime localDateTime = LocalDateTime.now();
            if (req.getPublicTime().before(Timestamp.valueOf(localDateTime))) {
                System.out.println("Public time is too early!");
                return false;
            }

            Long videoOwner = find_VideoOwner(bv);
            if (videoOwner == null || !videoOwner.equals(auth.getMid())) {
                return false; // 找不到 video 或者不是对应的 owner
            }

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmtVideoInfo = conn.prepareStatement(sqlVideoInformation);
                 PreparedStatement stmtUpdate = conn.prepareStatement(sqlUpdateVideo);
            ) {
                stmtVideoInfo.setString(1, bv);
                log.info("SQL: {}", stmtVideoInfo);
                ResultSet rsVideoInfo = stmtVideoInfo.executeQuery();
                if (!rsVideoInfo.next()) {
                    return false; // 没有找到对应的 video
                }

                String title = rsVideoInfo.getString(1);
                String description = rsVideoInfo.getString(2);
                float duration = rsVideoInfo.getFloat(3);
                Timestamp publicTime = rsVideoInfo.getTimestamp(4);
                Timestamp reviewTime = rsVideoInfo.getTimestamp(5);

                if (req.getDuration() != duration) {
                    return false; // duration 被改变
                }

                if (req.getDescription().equals(description) && req.getTitle().equals(title)
                        && req.getPublicTime().equals(publicTime)) {
                    return false; // 没有更改信息
                }

                stmtUpdate.setString(1, req.getTitle());
                stmtUpdate.setString(2, req.getDescription());
                stmtUpdate.setTimestamp(3, req.getPublicTime());
                stmtUpdate.setTimestamp(4, null); // 需要重新审核
                stmtUpdate.setLong(5, 0);
                stmtUpdate.setString(6, bv);
                log.info("SQL: {}", stmtUpdate);
                stmtUpdate.executeUpdate(); // 执行更新操作

                return reviewTime != null; // 返回是否需要重新审核
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if (!CheckAuthoInfo(auth)) {
            System.out.println("Autho Information is wrong!");
            return null;
        }

        if (keywords == null || keywords.isEmpty()) {
            System.out.println("Keywords are null!");
            return null;
        }

        if (pageNum <= 0 || pageSize <= 0) {
            System.out.println("Page is wrong!");
            return null;
        }

        int relevance = 0; // 关键词相关程度
        String[] keywordsGroup = keywords.split(" ");
        List<VideoRecord> results = new ArrayList<>();
        LocalDateTime rightNow = LocalDateTime.now();
        String sqlTitleSearch = "SELECT bv, viewermids, reviewtime, title, publictime, ownermid FROM \"VideoRecord\" WHERE title ILIKE ?";
        String sqlUserIdentity = "SELECT identity FROM \"UserRecord\" WHERE mid = ?";
        String sqlKeywordSearch = "SELECT bv, viewermids, reviewtime, title, publictime, ownermid FROM \"VideoRecord\" WHERE description ILIKE ?";
        String sqlSearchByOwnerName = "SELECT bv, viewermids, reviewtime, title, publictime, ownermid FROM \"VideoRecord\" WHERE ownername ILIKE ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtUserIdentity = conn.prepareStatement(sqlUserIdentity);
             PreparedStatement stmtSearchByTitle = conn.prepareStatement(sqlTitleSearch);
             PreparedStatement stmtKeywordSearch = conn.prepareStatement(sqlKeywordSearch);
             PreparedStatement stmtSearchByOwnerName = conn.prepareStatement(sqlSearchByOwnerName);
        ) {
            stmtUserIdentity.setLong(1, auth.getMid());
            log.info("SQL: {}", sqlUserIdentity);
            ResultSet rsUserIdentity = stmtUserIdentity.executeQuery();
            rsUserIdentity.next();
            String identity = rsUserIdentity.getString(1); // 获取当前用户的身份信息

            for (String key : keywordsGroup) {
                searchAndAddResults(stmtSearchByTitle, results, key, rightNow, auth.getMid(), identity);
            }

            for (String key : keywordsGroup) {
                searchAndAddResults(stmtKeywordSearch, results, key, rightNow, auth.getMid(), identity);
            }

            for (String key : keywordsGroup) {
                searchAndAddResults(stmtSearchByOwnerName, results, key, rightNow, auth.getMid(), identity);
            }

            // 添加完毕，最后根据相关度和播放量进行排序
            results.sort((record1, record2) -> {
                int relevanceComparison = Integer.compare(record2.getRelevance(), record1.getRelevance());
                if (relevanceComparison != 0) {
                    return relevanceComparison;
                } else {
                    return Long.compare(record2.getViewer_num(), record1.getViewer_num());
                }
            });

            // 排序完毕，根据 pageNumber 和 pageSize 返回结果
            List<String> finalResults = new ArrayList<>();
            for (VideoRecord videoRecord : results) {
                finalResults.add(videoRecord.getBv());
            }

            return getPaginatedResults(finalResults, pageNum, pageSize);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Helper method to search and add results based on the given PreparedStatement
    private void searchAndAddResults(PreparedStatement statement, List<VideoRecord> results, String keyword,
                                     LocalDateTime rightNow, long authMid, String identity) throws SQLException {
        statement.setString(1, "%" + keyword + "%");
        log.info("SQL: {}", statement);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            boolean alreadyExist = false;
            if ((resultSet.getTimestamp(3) != null && resultSet.getTimestamp(5).before(Timestamp.valueOf(rightNow)))
                    || identity.equalsIgnoreCase("SUPERUSER") || authMid == resultSet.getLong(6)) {
                for (VideoRecord videoRecord : results) {
                    if (videoRecord.getBv().equals(resultSet.getString(1))) {
                        videoRecord.setRelevance(videoRecord.getRelevance() + 1);
                        alreadyExist = true;
                        break;
                    }
                }
                if (!alreadyExist) {
                    VideoRecord newRecord = new VideoRecord();
                    Array ss = resultSet.getArray(2);
                    Object[] array = (Object[]) ss.getArray();
                    long size = array.length;
                    newRecord.setBv(resultSet.getString(1));
                    newRecord.setViewer_num(size);
                    newRecord.setRelevance(1);
                    results.add(newRecord);
                }
            }
        }
    }


    //该方法根据用户要求返回指定页数的子列表
    public List<String> getPaginatedResults(List<String> results, int pageNumber, int pageSize) {
        int startIndex = (pageNumber - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, results.size());

        if (startIndex >= endIndex) {
            return new ArrayList<>(); // 如果请求的页面为空，则返回一个空列表
        }

        return results.subList(startIndex, endIndex);
    }


    @Override
    public double getAverageViewRate(String bv) {
        if (find_VideoOwner(bv) == null) {
            return -1;//找不到对应bv的视频
        }
        String sql_find_user_viewTime = "Select * from view_video where bv = ?";
        String sql_duration = "Select duration from \"VideoRecord\" where bv = ? ";
        double sum = 0;
        double avg_view_rate = 0;
        int count_users = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_user_viewTime = conn.prepareStatement(sql_find_user_viewTime);
             PreparedStatement stmt_duration = conn.prepareStatement(sql_duration);
        ) {
            stmt_duration.setString(1, bv);
            log.info("SQL: {}", stmt_duration);
            ResultSet rs_duration = stmt_duration.executeQuery();
            float duration = 0;
            if (!rs_duration.wasNull()) {
                rs_duration.next();
                duration = rs_duration.getFloat(1);
            }
            stmt_find_user_viewTime.setString(1, bv);
            log.info("SQL: {}", stmt_find_user_viewTime);
            ResultSet rs_find_user_viewTime = stmt_find_user_viewTime.executeQuery();
            if (rs_find_user_viewTime.wasNull()) {
                return -1;//没有人看该Video
            } else {
                while (rs_find_user_viewTime.next()) {
                    sum = sum + (double) rs_find_user_viewTime.getFloat(1);
                    count_users++;
                }
            }
            avg_view_rate = sum / (count_users * duration);
            return avg_view_rate;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        if (find_VideoOwner(bv) == null) {
            return null;
        }

        Set<Integer> hotspotChunks = new HashSet<>();

        String sql_check_danmu = "SELECT id, time FROM \"DanmuRecord\" WHERE bv = ? ORDER BY time";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_check_danmu = conn.prepareStatement(sql_check_danmu)) {
            stmt_check_danmu.setString(1, bv);
            log.info("SQL: {}", stmt_check_danmu);
            ResultSet rs_check_danmu = stmt_check_danmu.executeQuery();

            if (!rs_check_danmu.next()) {
                // 结果集中没有行，意味着没有人在该视频上面发弹幕
                return null;
            } else {
                int currentChunk = 0;
                int currentChunkDanmuCount = 0;
                int maxChunk = 0;
                int maxChunkDanmuCount = 0;

                do {
                    long time = rs_check_danmu.getLong("time");
                    int chunk = (int) (time / 10000);

                    if (chunk == currentChunk) {
                        // 在同一区间内，增加弹幕数量
                        currentChunkDanmuCount++;
                    } else {
                        // 进入新的区间，判断是否是最多弹幕的区间
                        if (currentChunkDanmuCount > maxChunkDanmuCount) {
                            maxChunkDanmuCount = currentChunkDanmuCount;
                            maxChunk = currentChunk;
                        }
                        // 重置当前区间
                        currentChunk = chunk;
                        currentChunkDanmuCount = 1;
                    }

                } while (rs_check_danmu.next());
                // 处理最后一个区间
                if (currentChunkDanmuCount > maxChunkDanmuCount) {
                    maxChunk = currentChunk;
                }
                hotspotChunks.add(maxChunk);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return hotspotChunks;
    }

    public String findUserIdentity(AuthInfo auth) {
        String sqlFindUserIdentity = "SELECT identity FROM \"UserRecord\" WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtFindUserIdentity = conn.prepareStatement(sqlFindUserIdentity);
        ) {
            stmtFindUserIdentity.setLong(1, auth.getMid());
            log.info("SQL: {}", stmtFindUserIdentity);
            ResultSet rsFindUserIdentity = stmtFindUserIdentity.executeQuery();

            if (rsFindUserIdentity.next()) {
                return rsFindUserIdentity.getString(1);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (!CheckAuthoInfo(auth)) {
            return false;
        }

        if (!findUserIdentity(auth).equalsIgnoreCase("USER") || find_VideoOwner(bv) == auth.getMid()) {
            return false;
        }

        String sql_find_reviewtime = "SELECT reviewtime FROM \"VideoRecord\" WHERE bv = ?";
        String sql_update_review = "UPDATE \"VideoRecord\" SET reviewtime = ?, reviewer = ? WHERE bv = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_reviewtime = conn.prepareStatement(sql_find_reviewtime);
             PreparedStatement stmt_update_review = conn.prepareStatement(sql_update_review);
        ) {
            stmt_find_reviewtime.setString(1, bv);
            log.info("SQL: {}", sql_find_reviewtime);
            ResultSet rs_find_reviewtime = stmt_find_reviewtime.executeQuery();

            if (!rs_find_reviewtime.next()) {// 结果集中没有行，说明没有被Review过，需要将审核时间更新为当前的时间。
                stmt_find_reviewtime.close();  // 关闭之前的查询，释放资源

                LocalDateTime localDateTime = LocalDateTime.now();
                stmt_update_review.setTimestamp(1, Timestamp.valueOf(localDateTime));
                stmt_update_review.setLong(2, auth.getMid());
                stmt_update_review.setString(3, bv);

                log.info("SQL: {}", stmt_update_review);
                int rowsUpdated = stmt_update_review.executeUpdate();

                return rowsUpdated > 0; // 如果更新的行数大于0，则更新成功
            } else {
                return false;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean can_search(AuthInfo auth, String bv) {
        String sql_video_info = "SELECT reviewtime, ownermid, publictime FROM \"VideoRecord\" WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_video_info = conn.prepareStatement(sql_video_info);
        ) {
            if (!CheckAuthoInfo(auth)) {
                return false;
            }

            stmt_video_info.setString(1, bv);
            log.info("SQL: {}", stmt_video_info);
            ResultSet rs_video_info = stmt_video_info.executeQuery();

            if (!rs_video_info.next()) { // 没有查询到结果行
                return false;
            } else {
                Timestamp reviewTime = rs_video_info.getTimestamp(1);
                long ownerMid = rs_video_info.getLong(2);
                Timestamp publicTime = rs_video_info.getTimestamp(3);

                if (reviewTime == null) {
                    return false;
                }

                LocalDateTime localDateTime = LocalDateTime.now();

                if (publicTime != null && publicTime.after(Timestamp.valueOf(localDateTime))) {
                    return false;
                }

                if (ownerMid == auth.getMid() || findUserIdentity(auth).equalsIgnoreCase("USER")) {
                    return false;
                }
            }

            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        // 只有当用户能够搜索到 video 的时候，才能投币
        String sqlVideoInfo = "SELECT reviewtime, ownermid, publictime FROM \"VideoRecord\" WHERE bv = ?";
        String sqlCoin = "SELECT * FROM coin_user WHERE bv = ? AND coin_mid = ?";
        String sqlUserCoin = "SELECT coin FROM \"UserRecord\" WHERE mid = ?";
        String sqlUpdateUserRecord = "UPDATE \"UserRecord\" SET coin = ? WHERE mid = ?";
        String sqlInsertCoinUser = "INSERT INTO coin_user (bv, coin_mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtVideoInfo = conn.prepareStatement(sqlVideoInfo);
             PreparedStatement stmtCoin = conn.prepareStatement(sqlCoin);
             PreparedStatement stmtUserCoin = conn.prepareStatement(sqlUserCoin);
             PreparedStatement stmtUpdateUserRecord = conn.prepareStatement(sqlUpdateUserRecord);
             PreparedStatement stmtInsertCoinUser = conn.prepareStatement(sqlInsertCoinUser);
        ) {
            if (!CheckAuthoInfo(auth)) {
                return false;
            }

            stmtVideoInfo.setString(1, bv);
            log.info("SQL: {}", stmtVideoInfo);
            ResultSet rsVideoInfo = stmtVideoInfo.executeQuery();

            if (!rsVideoInfo.next()) {
                return false; // 未找到对应的视频
            }

            if (rsVideoInfo.getTimestamp(1) == null || rsVideoInfo.getTimestamp(3).after(Timestamp.valueOf(LocalDateTime.now()))) {
                return false; // 视频未审核或未公开
            }

            if (rsVideoInfo.getLong(2) == auth.getMid() || findUserIdentity(auth).equalsIgnoreCase("USER")) {
                return false; // 用户是视频所有者或无法搜索该视频
            }

            stmtUserCoin.setLong(1, auth.getMid());
            log.info("SQL: {}", stmtUserCoin);
            ResultSet rsUserCoin = stmtUserCoin.executeQuery();

            if (!rsUserCoin.next() || rsUserCoin.getInt(1) <= 0) {
                return false; // 用户没有 coin
            }

            stmtCoin.setString(1, bv);
            stmtCoin.setLong(2, auth.getMid());
            log.info("SQL: {}", stmtCoin);
            ResultSet rsCoin = stmtCoin.executeQuery();

            if (!rsCoin.next()) { // 用户没有投过币
                int updatedUserCoin = rsUserCoin.getInt(1) - 1;
                stmtUpdateUserRecord.setInt(1, updatedUserCoin);
                stmtUpdateUserRecord.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtUpdateUserRecord);
                stmtUpdateUserRecord.executeUpdate();

                stmtInsertCoinUser.setString(1, bv);
                stmtInsertCoinUser.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtInsertCoinUser);
                stmtInsertCoinUser.executeUpdate();

                return true;
            } else {
                return false; // 用户已经投过币或无法撤回投币
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        if (!can_search(auth, bv)) {
            return false;
        }

        String sqlCheckLike = "SELECT * FROM video_like WHERE bv = ? AND video_like_mid = ?";
        String sqlUpdateLikeVideoRecord = "UPDATE \"VideoRecord\" SET \"Like\" = array_cat(\"Like\", Array[?]) WHERE bv = ?";
        String sqlUpdateVideoLike = "UPDATE \"VideoRecord\" SET \"Like\" = array_cat(\"Like\", Array[?]) WHERE bv = ?";
        String sqlDeleteLikeVideoRecord = "UPDATE \"VideoRecord\" SET \"Like\" = array_remove(\"Like\", ?) WHERE bv = ?";
        String sqlDeleteLikeVideoLike = "DELETE FROM video_like WHERE bv = ? AND video_like_mid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtCheckLike = conn.prepareStatement(sqlCheckLike);
             PreparedStatement stmtUpdateLikeVideoRecord = conn.prepareStatement(sqlUpdateLikeVideoRecord);
             PreparedStatement stmtUpdateVideoLike = conn.prepareStatement(sqlUpdateVideoLike);
             PreparedStatement stmtDeleteLikeVideoRecord = conn.prepareStatement(sqlDeleteLikeVideoRecord);
             PreparedStatement stmtDeleteLikeVideoLike = conn.prepareStatement(sqlDeleteLikeVideoLike);
        ) {
            stmtCheckLike.setString(1, bv);
            stmtCheckLike.setLong(2, auth.getMid());
            log.info("SQL: {}", stmtCheckLike);
            ResultSet rsCheckLike = stmtCheckLike.executeQuery();

            if (!rsCheckLike.next()) {// 用户之前没有 Like 过
                stmtUpdateLikeVideoRecord.setLong(1, auth.getMid());
                stmtUpdateLikeVideoRecord.setString(2, bv);
                log.info("SQL: {}", stmtUpdateLikeVideoRecord);
                stmtUpdateLikeVideoRecord.executeUpdate();

                stmtUpdateVideoLike.setString(1, bv);
                stmtUpdateVideoLike.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtUpdateVideoLike);
                stmtUpdateVideoLike.executeUpdate();

                return true;
            } else {
                stmtDeleteLikeVideoRecord.setLong(1, auth.getMid());
                stmtDeleteLikeVideoRecord.setString(2, bv);
                log.info("SQL: {}", stmtDeleteLikeVideoRecord);
                stmtDeleteLikeVideoRecord.executeUpdate();

                stmtDeleteLikeVideoLike.setString(1, bv);
                stmtDeleteLikeVideoLike.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtDeleteLikeVideoLike);
                stmtDeleteLikeVideoLike.executeUpdate();

                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        if (!can_search(auth, bv)) {
            return false;
        }

        String sqlCheckCollect = "SELECT * FROM video_collect WHERE bv = ? AND collected_mid = ?";
        String sqlUpdateCollectVideoRecord = "UPDATE \"VideoRecord\" SET favorite = array_cat(favorite, Array[?]) WHERE bv = ?";
        String sqlUpdateCollectVideo = "INSERT INTO video_collect (bv, collected_mid) VALUES (?, ?)";
        String sqlDeleteCollectVideoRecord = "UPDATE \"VideoRecord\" SET favorite = array_remove(favorite, ?) WHERE bv = ?";
        String sqlDeleteCollectVideo = "DELETE FROM video_collect WHERE collected_mid = ? AND bv = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtCheckCollect = conn.prepareStatement(sqlCheckCollect);
             PreparedStatement stmtUpdateCollectVideoRecord = conn.prepareStatement(sqlUpdateCollectVideoRecord);
             PreparedStatement stmtUpdateCollectVideo = conn.prepareStatement(sqlUpdateCollectVideo);
             PreparedStatement stmtDeleteCollectVideoRecord = conn.prepareStatement(sqlDeleteCollectVideoRecord);
             PreparedStatement stmtDeleteCollectVideo = conn.prepareStatement(sqlDeleteCollectVideo);
        ) {
            stmtCheckCollect.setString(1, bv);
            stmtCheckCollect.setLong(2, auth.getMid());
            log.info("SQL: {}", stmtCheckCollect);
            ResultSet rsCheckCollect = stmtCheckCollect.executeQuery();

            if (!rsCheckCollect.next()) {// 用户之前没有收藏过
                stmtUpdateCollectVideoRecord.setLong(1, auth.getMid());
                stmtUpdateCollectVideoRecord.setString(2, bv);
                log.info("SQL: {}", stmtUpdateCollectVideoRecord);
                stmtUpdateCollectVideoRecord.executeUpdate();

                stmtUpdateCollectVideo.setString(1, bv);
                stmtUpdateCollectVideo.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtUpdateCollectVideo);
                stmtUpdateCollectVideo.executeUpdate();

                return true;
            } else {// 用户之前已经收藏过了，需要取消收藏
                stmtDeleteCollectVideoRecord.setLong(1, auth.getMid());
                stmtDeleteCollectVideoRecord.setString(2, bv);
                log.info("SQL: {}", stmtDeleteCollectVideoRecord);
                stmtDeleteCollectVideoRecord.executeUpdate();

                stmtDeleteCollectVideo.setString(1, bv);
                stmtDeleteCollectVideo.setLong(2, auth.getMid());
                log.info("SQL: {}", stmtDeleteCollectVideo);
                stmtDeleteCollectVideo.executeUpdate();

                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
