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
        String sql_mid = "Select Password from \"UserRecord\" where mid = ?";//密码是否匹配
        String sql_qq_wechat = "SELECT ((SELECT mid FROM \"UserRecord\" WHERE qq = ?) INTERSECT (SELECT mid FROM \"UserRecord\" WHERE wechat = ?))";//如果同时提供了qq和微信，是否能够匹配。
        String sql_qq = "Select mid from \"UserRecord\" where QQ = ?";
        String sql_wechat = "Select mid from \"UserRecord\" where wechat =?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_check_password = conn.prepareStatement(sql_mid);
             PreparedStatement stmt_check_qq_wechat_match = conn.prepareStatement(sql_qq_wechat);
             PreparedStatement stmt_checkqq_match = conn.prepareStatement(sql_qq);
             PreparedStatement stmt_check_wechat_match = conn.prepareStatement(sql_wechat);

        ) {
            stmt_check_password.setLong(1, auth.getMid());
            log.info("SQL: {}", stmt_check_password);
            stmt_check_qq_wechat_match.setString(1, auth.getQq());
            stmt_check_qq_wechat_match.setString(2, auth.getWechat());
            log.info("SQL: {}", stmt_check_qq_wechat_match);
            stmt_checkqq_match.setString(1, auth.getQq());
            log.info("SQL: {}", stmt_checkqq_match);
            stmt_check_wechat_match.setString(1, auth.getWechat());
            log.info("SQL: {}", stmt_check_wechat_match);
            ResultSet rs_check_password = stmt_check_password.executeQuery();
            rs_check_password.next();
            ResultSet rs_check_qq_wechat_match = stmt_check_qq_wechat_match.executeQuery();
            rs_check_qq_wechat_match.next();
            ResultSet rs_checkqq_match = stmt_checkqq_match.executeQuery();
//            rs_check_qq_wechat_match.next();
            ResultSet rs_check_wechat_match = stmt_check_wechat_match.executeQuery();
            rs_check_wechat_match.next();
            //如果没有提供任何登陆方式或者只提供了用户名或者密码，返回false
            if (auth.getQq() == null && auth.getWechat() == null && (auth.getMid() == 0 || auth.getPassword() == null)) {
                System.out.println("No log in information is provided!");
            }
            //只提供了qq或者微信，登陆成功
            if (auth.getMid() == 0 && auth.getPassword() == null && ((auth.getQq() != null && auth.getWechat() == null) || (auth.getQq() == null && auth.getWechat() != null))) {
                if (auth.getQq() != null) {
                    if (!rs_checkqq_match.wasNull()) {
                        System.out.println("Only qq is provided!");
                        return true;
                    }
                } else if (auth.getWechat() != null) {
                    if (!rs_check_wechat_match.wasNull()) {
                        System.out.println("Only wechat is provided!");
                        return true;
                    }
                }
            }
            //提供了两项信息，两个信息是同一个人的信息，可以登陆
            if (auth.getPassword() != null) {
                if (rs_check_password.getLong(1) != auth.getMid()) {
                    System.out.println("Mid and password don't match!");
                    return false;
                }
            }//密码与人不匹配
            if (auth.getQq() != null && auth.getPassword() != null) {
                if (rs_check_password.getLong(1) != rs_checkqq_match.getLong(1)) {
                    System.out.println("qq and password don't match!");
                    return false;
                }
            }//提供了密码和qq，但是二者不匹配。
            if (auth.getWechat() != null && auth.getPassword() != null) {
                if (rs_check_wechat_match.getLong(1) != rs_check_password.getLong(1)) {
                    System.out.println("wechat and password don't match!");
                    return false;
                }
            }//提供了密码和微信，但是两者不匹配
            if (auth.getWechat() != null && auth.getQq() != null) {
                if (rs_check_qq_wechat_match.wasNull()) {
                    System.out.println("qq and wechat don't match!");
                    return false;
                }
            }//提供了微信和qq，但是两者不匹配
            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    //如何生成独一无二的bv号？
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        String sql_sametitle_and_user = "SELECT COUNT(*) FROM \"VideoRecord\" v JOIN \"UserRecord\" u ON v.OwnerMid = u.mid WHERE v.Title = ? AND v.OwnerMid = ?";
        String sql_findmidname = "SELECT u.name FROM \"UserRecord\" u WHERE u.mid = ?";
        VideoRecord newvideoRecord = new VideoRecord();
        if (!CheckAuthoInfo(auth)) {
            System.out.println("Authinfo is wrong!");
            return null;
        } else {
            if (req.getTitle() == null) {
                System.out.println("title is null!");
                return null;
            }
            if (req.getDuration() < 10) {
                System.out.println("duration is too short!");
                return null;
            }
            LocalDateTime localDateTime = LocalDateTime.now();
            if (req.getPublicTime().before(Timestamp.valueOf(localDateTime))) {
                System.out.println("Public time is too early!");
                return null;
            }
            String sql_insertvideo = "Insert into \"VideoRecord\" (bv, title, ownermid, ownername, committime, duration, description)\n" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (Connection conn = dataSource.getConnection();
                 Connection conn_insertvideo = dataSource.getConnection();
                 PreparedStatement stmt_sametitle_and_user = conn.prepareStatement(sql_sametitle_and_user);
                 PreparedStatement stmt_findmidname = conn.prepareStatement(sql_findmidname);
                 PreparedStatement stmt_insertuser = conn_insertvideo.prepareStatement(sql_insertvideo);

            ) {
                stmt_sametitle_and_user.setString(1, req.getTitle());
                stmt_sametitle_and_user.setLong(2, auth.getMid());
                log.info("SQL: {}", stmt_sametitle_and_user);
                ResultSet rs_sametitle_and_user = stmt_sametitle_and_user.executeQuery();
                rs_sametitle_and_user.next();
                if (rs_sametitle_and_user.getInt(1) != 0) {
                    System.out.println("There is already a same video!");
                    return null;
                }
                stmt_findmidname.setLong(1, auth.getMid());
                log.info("SQL: {}", stmt_findmidname);
                ResultSet rs_findmidname = stmt_findmidname.executeQuery();
                rs_findmidname.next();
                String authname = rs_findmidname.getString(1);
                newvideoRecord.setBv(generatebv());
                newvideoRecord.setTitle(req.getTitle());
                newvideoRecord.setOwnerMid(auth.getMid());
                newvideoRecord.setOwnerName(authname);
                newvideoRecord.setCommitTime(Timestamp.valueOf(localDateTime));
                newvideoRecord.setDuration(req.getDuration());
                newvideoRecord.setDescription(req.getDescription());
                newvideoRecord.setVideo_is_Deleted(false);
                stmt_insertuser.setString(1, newvideoRecord.getBv());
                stmt_insertuser.setString(2, newvideoRecord.getTitle());
                stmt_sametitle_and_user.setLong(3, newvideoRecord.getOwnerMid());
                stmt_sametitle_and_user.setString(4, newvideoRecord.getOwnerName());
                stmt_sametitle_and_user.setTimestamp(5, newvideoRecord.getCommitTime());
                stmt_sametitle_and_user.setFloat(6, newvideoRecord.getDuration());
                stmt_sametitle_and_user.setString(7, newvideoRecord.getDescription());
                log.info("SQL: {}", stmt_insertuser);
                stmt_sametitle_and_user.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return newvideoRecord.getBv();
    }

    public String generatebv() {
        return "BV2".concat(String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        boolean authinfo = CheckAuthoInfo(auth);
        if (authinfo == false) {
            return false;
        }
        String sql_user_identiey = "Select identity from \"UserRecord\" where mid = ?";
        String sql_video = "Select ownerMid from \"VideoRecord\" where bv = ?";
        String sql_delete = "Delete from \"VideoRecord\" where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_user_identity = conn.prepareStatement(sql_user_identiey);
             PreparedStatement stmt_video = conn.prepareStatement(sql_video);
             PreparedStatement stmt_delete_video = conn.prepareStatement(sql_delete);
        ) {
            stmt_user_identity.setLong(1, auth.getMid());
            log.info("SQL: {}", stmt_user_identity);
            ResultSet rs_user_identity = stmt_user_identity.executeQuery();
            rs_user_identity.next();
            String identity = rs_user_identity.getString(1);//查询到用户的身份
            stmt_video.setString(1, bv);
            log.info("SQL: {}", stmt_video);
            ResultSet rs_video = stmt_video.executeQuery();
            Long ownermid;
            if (rs_video.wasNull()) {
                return false;
            }//没有找到对应的video
            else {
                rs_video.next();
                ownermid = rs_video.getLong(1);
                if (ownermid != auth.getMid() && String.valueOf(identity).equals("USER")) {
                    return false;
                }
            }//不是video对应的owner且不是超级用户
            stmt_delete_video.setString(1, bv);
            log.info("SQL: {}", stmt_delete_video);
            stmt_delete_video.executeUpdate();//删除videoRecord中的记录
            //TODO:删除相关的项
            return true;


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Long find_VideoOwner(String bv) {
        String sql_find_owner = "Select ownerMid from \"VideoRecord\" where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_owner = conn.prepareStatement(sql_find_owner);

        ) {
            stmt_find_owner.setString(1, bv);
            log.info("SQL: {}", stmt_find_owner);
            ResultSet rs_findowner = stmt_find_owner.executeQuery();
            if (rs_findowner.wasNull()) {
                return null;
            } else {
                rs_findowner.next();
                return rs_findowner.getLong(1);
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO:需要superuser重新审核
    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        String sql_video_information = "Select title,description,duration,publicTime from \"VideoRecord\" where bv = ?";
        String sql_update_video = "UPDATE \"VideoRecord\" set title = ?,description = ?, publicTime = ?,reviewtime = ? where bv = ?";
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
            }//用户认证信息和视频信息不正确
            if (find_VideoOwner(bv) == null) {
                return false;
            } else if (find_VideoOwner(bv) != auth.getMid()) {
                return false;
            }
            //找不到video或者不是对应的owner
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt_video_info = conn.prepareStatement(sql_video_information);
                 PreparedStatement stmt_update = conn.prepareStatement(sql_update_video);
            ) {
                stmt_video_info.setString(1, bv);
                log.info("SQL: {}", stmt_video_info);
                ResultSet rs_video_info = stmt_video_info.executeQuery();
                if (rs_video_info.wasNull()) {
                    return false;
                } else {
                    rs_video_info.next();
                }
                String title = rs_video_info.getString(1);
                String description = rs_video_info.getString(2);
                float duration = rs_video_info.getFloat(3);
                Timestamp publictime = rs_video_info.getTimestamp(4);
                if (req.getDuration() != duration) {
                    return false;
                }//duration 被改变
                if (req.getDescription() == description && req.getTitle() == title && req.getPublicTime() == publictime) {
                    return false;
                }//没有更改信息。
                stmt_update.setString(1, req.getTitle());
                stmt_update.setString(2, req.getDescription());
                stmt_update.setTimestamp(3, req.getPublicTime());
                stmt_update.setString(5, bv);
                stmt_update.setTimestamp(4, null);
                log.info("SQL: {}", stmt_update);
                stmt_update.executeUpdate();//执行更新操作
                //TODO:是否需要对其他地方进行更新
                return true;
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
        }//用户认证错误
        if (keywords.isEmpty() || keywords == null) {
            System.out.println("keywords are null!");
            return null;//关键字为空！
        }
        if (pageNum <= 0 || pageSize <= 0) {
            System.out.println("Page is wrong!");
            return null;//pagesize有问题
        }
        int relevance = 0; //关键词相关程度
        String[] keywords_group = keywords.split(" ");
        List<VideoRecord> results = new ArrayList<>();
        LocalDateTime rightnow = LocalDateTime.now();
        String sql_title_search = "Select bv,viewermids,reviewtime,title,publictime,ownermid from \"VideoRecord\" where title ILIKE ?";
        String sql_user_identity = "Select identity from \"UserRecord\" where mid = ?";
        String sql_keyword_search = "Select bv,viewermids,reviewtime,title,publictime,ownermid from \"VideoRecord\" where description ILIKE ?";
        String sql_search_by_owner_name = "Select bv,viewermids,reviewtime,title,publictime,ownermid from \"VideoRecord\" where ownername ILIKE ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_search_by_title = conn.prepareStatement(sql_title_search);
             PreparedStatement stmt_user_identity = conn.prepareStatement(sql_user_identity);
             PreparedStatement stmt_keyword_search = conn.prepareStatement(sql_keyword_search);
             PreparedStatement stmt_search_by_owner_name = conn.prepareStatement(sql_search_by_owner_name);

        ) {
            stmt_user_identity.setLong(1, auth.getMid());
            log.info("SQL: {}", sql_user_identity);
            ResultSet rs_user_identity = stmt_user_identity.executeQuery();
            rs_user_identity.next();
            String identity = rs_user_identity.getString(1);//获取当前用户的身份信息

            for (String key : keywords_group) {
                stmt_search_by_title.setString(1, "'%" + key + "%'");
                log.info("SQL: {}", stmt_search_by_title);
                ResultSet rs_search_by_title = stmt_search_by_title.executeQuery();
                if (!rs_search_by_title.wasNull()) {
                    while (rs_search_by_title.next()) {
                        boolean already_exist = false;
                        if ((rs_search_by_title.getTimestamp(3) != null && rs_search_by_title.getTimestamp(5).before(Timestamp.valueOf(rightnow))) || identity.equalsIgnoreCase("SUPERUSER") || auth.getMid() == rs_search_by_title.getLong(6)) {//如果是超级用户或者视频的主人，或者本身视频满足“要求”，就可以搜索到这个视频
                            for (VideoRecord videorecord : results) {
                                if (videorecord.getBv().equals(rs_search_by_title.getString(1))) {
                                    //如果在结果的表中已经有匹配了，只用对relevance加一,并且将already_exist设为真。
                                    videorecord.setRelevance(videorecord.getRelevance() + 1);
                                    already_exist = true;
                                    break;//TODO:是否能break?
                                }
                            }
                            if (!already_exist) {
                                VideoRecord newrecord = new VideoRecord();
                                Array ss = rs_search_by_title.getArray(2);
                                Object[] array = (Object[]) ss.getArray();
                                long size = array.length;
                                newrecord.setBv(rs_search_by_title.getString(1));
                                newrecord.setViewer_num(size);
                                newrecord.setRelevance(1);
                                results.add(newrecord);
                            }
                        }
                    }
                }
            }
            for (String key : keywords_group) {
                stmt_keyword_search.setString(1, "'%" + key + "%'");
                log.info("SQL: {}", stmt_keyword_search);
                ResultSet rs_search_by_keyword = stmt_keyword_search.executeQuery();
                if (!rs_search_by_keyword.wasNull()) {
                    while (rs_search_by_keyword.next()) {
                        boolean already_exist = false;
                        if ((rs_search_by_keyword.getTimestamp(3) != null && rs_search_by_keyword.getTimestamp(5).before(Timestamp.valueOf(rightnow))) || identity.equalsIgnoreCase("SUPERUSER") || auth.getMid() == rs_search_by_keyword.getLong(6)) {//如果是超级用户或者视频的主人，或者本身视频满足“要求”，就可以搜索到这个视频
                            for (VideoRecord videorecord : results) {
                                if (videorecord.getBv().equals(rs_search_by_keyword.getString(1))) {
                                    //如果在结果的表中已经有匹配了，只用对relevance加一,并且将already_exist设为真。
                                    videorecord.setRelevance(videorecord.getRelevance() + 1);
                                    already_exist = true;
                                    break;//TODO:是否能break?
                                }
                            }
                            if (!already_exist) {
                                VideoRecord newrecord = new VideoRecord();
                                Array ss = rs_search_by_keyword.getArray(2);
                                Object[] array = (Object[]) ss.getArray();
                                long size = array.length;
                                newrecord.setBv(rs_search_by_keyword.getString(1));
                                newrecord.setViewer_num(size);
                                newrecord.setRelevance(1);
                                results.add(newrecord);
                            }
                        }
                    }
                }
            }
            for (String key : keywords_group) {
                stmt_search_by_owner_name.setString(1, "'%" + key + "%'");
                log.info("SQL: {}", stmt_search_by_owner_name);
                ResultSet rs_search_by_owner_name = stmt_search_by_owner_name.executeQuery();
                if (!rs_search_by_owner_name.wasNull()) {
                    while (rs_search_by_owner_name.next()) {
                        boolean already_exist = false;
                        if ((rs_search_by_owner_name.getTimestamp(3) != null && rs_search_by_owner_name.getTimestamp(5).before(Timestamp.valueOf(rightnow))) || identity.equalsIgnoreCase("SUPERUSER") || auth.getMid() == rs_search_by_owner_name.getLong(6)) {//如果是超级用户或者视频的主人，或者本身视频满足“要求”，就可以搜索到这个视频
                            for (VideoRecord videorecord : results) {
                                if (videorecord.getBv().equals(rs_search_by_owner_name.getString(1))) {
                                    //如果在结果的表中已经有匹配了，只用对relevance加一,并且将already_exist设为真。
                                    videorecord.setRelevance(videorecord.getRelevance() + 1);
                                    already_exist = true;
                                    break;//TODO:是否能break?
                                }
                            }
                            if (!already_exist) {
                                VideoRecord newrecord = new VideoRecord();
                                Array ss = rs_search_by_owner_name.getArray(2);
                                Object[] array = (Object[]) ss.getArray();
                                long size = array.length;
                                newrecord.setBv(rs_search_by_owner_name.getString(1));
                                newrecord.setViewer_num(size);
                                newrecord.setRelevance(1);
                                results.add(newrecord);
                            }
                        }
                    }
                }
            }
            //添加完毕，最后根据相关度和播放量进行排序
            results.sort((record1, record2) -> {
                int relevanceComparison = Integer.compare(record2.getRelevance(), record1.getRelevance());
                if (relevanceComparison != 0) {
                    return relevanceComparison;
                } else {
                    return Long.compare(record2.getViewer_num(), record1.getViewer_num());
                }
            });
            List<String> final_results = new ArrayList<>();
            for (VideoRecord videoRecord : results) {
                final_results.add(videoRecord.getBv());
            }
            //排序完毕，根据pagenumber和pagesize返回值。
            return getPaginatedResults(final_results, pageNum, pageSize);
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
        return null;
    }

    public String find_user_identity(AuthInfo auth) {
        String sql_find_user_identity = "Select identity from \"UserRecord\" where mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_user_identity = conn.prepareStatement(sql_find_user_identity);

        ) {
            stmt_find_user_identity.setLong(1, auth.getMid());
            log.info("SQL: {}", stmt_find_user_identity);
            ResultSet rs_find_user_identity = stmt_find_user_identity.executeQuery();
            if (!rs_find_user_identity.wasNull()) {
                return rs_find_user_identity.getString(1);
            } else return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (!CheckAuthoInfo(auth)) {
            return false;
        }
        if (!find_user_identity(auth).equalsIgnoreCase("USER") || find_VideoOwner(bv) == auth.getMid()) {
            return false;
        }
        String sql_find_reviewtime = "Select reviewtime from \"VideoRecord\" where bv = ?";
        String sql_update_review = "UPDATE \"VideoRecord\" set reviewtime = ?, reviewer = ?  where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_reviewtime = conn.prepareStatement(sql_find_reviewtime);
             PreparedStatement stmt_update_review = conn.prepareStatement(sql_update_review);
        ) {
            stmt_find_reviewtime.setString(1, bv);
            log.info("SQL: {}", sql_find_reviewtime);
            ResultSet rs_find_reviewtime = stmt_find_reviewtime.executeQuery();
            if (rs_find_reviewtime.wasNull()) {//当没有被Review过，需要将审核时间更新为当前的时间。
                stmt_find_reviewtime.setString(3, bv);
                LocalDateTime localDateTime = LocalDateTime.now();
                stmt_update_review.setTimestamp(1, Timestamp.valueOf(localDateTime));
                stmt_update_review.setLong(2, auth.getMid());
                log.info("SQL: {}", stmt_update_review);
                stmt_update_review.executeUpdate();
                return true;
            } else {
                rs_find_reviewtime.next();
                return false;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }
}
